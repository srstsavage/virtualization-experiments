#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "icechunk==2.3.1",
#     "virtualizarr[hdf]==2.8.0",
#     "xarray==2026.9.0",
#     "zarr==3.4.2",
#     "obstore==0.12.1",
#     "obspec-utils==0.9.0",
#     "pyyaml==6.0.3",
#     "python-dotenv==1.1.1",
#     "topozarr==0.3.0",
# ]
# ///
"""Create or update a virtual Icechunk store over gridded NetCDF4/HDF5 files in
a public cloud bucket. Only metadata and byte-range references are written; the
science data stays where the provider put it.

Examples
--------
    # Create a store for October 2026 of the WCOFS nowcast fields
    ./virtualize.py wcofs-fields-nowcast --repo ./wcofs.icechunk \\
        --start 2026-10-01 --end 2026-10-31

    # Later: extend the same store (files already indexed are skipped)
    ./virtualize.py wcofs-fields-nowcast --repo ./wcofs.icechunk \\
        --start 2026-11-01 --end 2026-11-14

    # Re-ingest days already in the store (e.g. after the provider replaced files)
    ./virtualize.py wcofs-fields-nowcast --repo ./wcofs.icechunk \\
        --start 2026-10-15 --end 2026-10-16 --force

    # Cut at hours instead of whole days (valid times, UTC, both inclusive)
    ./virtualize.py wcofs-fields-nowcast --repo ./wcofs.icechunk \\
        --start 2026-11-15T00:00 --end 2026-11-15T12:00

    # See what would be indexed, without touching the repository
    ./virtualize.py wcofs-fields-nowcast --start 2026-10-01 --end 2026-10-03 --dry-run

    # Same, but into the local versitygw S3 container (docker compose up -d)
    ./virtualize.py wcofs-fields-nowcast --repo s3://icechunk/wcofs.icechunk \\
        --start 2026-10-01 --end 2026-10-31

    # Regular-grid files, with multiscale overviews for web maps (preset default)
    ./virtualize.py wcofs-regulargrid-nowcast --repo s3://icechunk/wcofs-rg.icechunk \\
        --start 2026-11-01 --end 2026-11-07

    # Best time series: nowcast, then the newest forecast. Re-run e.g. hourly;
    # steps are upgraded as better files appear, otherwise nothing happens
    ./virtualize.py wcofs-regulargrid-best --repo s3://icechunk/wcofs-rg-best.icechunk \\
        --start today-1

    # Rebuild the gridlook/Xreds catalogs from every repository in a root, then exit
    ./virtualize.py --scan-catalogs s3://icechunk

Catalogs: after a build, the repository's entry in gridlook.json and
xreds.yml (in _catalogs/ beside the repository) is created
or updated; other entries are kept (--no-catalog to skip).

Destination (--repo): a local directory, or s3://bucket/prefix. For s3:// the
standard variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_ENDPOINT_URL
(any S3-compatible server; http:// allowed, path-style addressing) are read from
the environment, and from a .env file next to this script for anything the
environment does not set. Credentials are never stored in this file.

Backblaze B2: for backblazeb2.com S3 endpoints, conditional writes are disabled
automatically and this setting is saved in the repository. Allow only one writer
at a time, including builds and garbage collection. AWS_RESPONSE_CHECKSUM_VALIDATION
defaults to WHEN_REQUIRED for B2's empty 304 responses; explicit environment/.env
values win. Other services can use --unsafe-no-conditional-writes explicitly.

Flaky networks: overview builds and validation read real bytes from the
source. If they fail with connect timeouts, lower --source-concurrency (e.g.
8..16) or set source_concurrency in the preset; see also --source-* timeouts
and retries. Each batch is committed, so a smaller --batch-size loses less
work on a failure, and re-running resumes (indexed files are skipped).

Snapshots: after a build that committed, snapshots older than --retain
(default 3d) are expired and the data only they used is deleted (--no-gc to
keep everything). Plain presets then lose the record of which files expired
commits indexed, so re-running an old window reopens those files (metadata
only) and skips them by their valid times.

Adding another dataset: add an entry to datasets.yml (next to this script, or
pass --config to use another file), or override any field of
an existing entry from the command line (--key-template, --file-regex, ...).

Reading the result:

    import icechunk as ic, xarray as xr, zarr
    zarr.config.set({"async.concurrency": 128})
    repo = ic.Repository.open(ic.local_filesystem_storage("./wcofs.icechunk"),
        authorize_virtual_chunk_access={PREFIX: ic.credentials.HttpAccess})  # PREFIX: see ds.attrs
    ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False, chunks={})
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import fnmatch
import importlib.metadata as md
import json
import logging
import math
import multiprocessing
import os
import re
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import icechunk as ic
import numpy as np
import xarray as xr
import yaml
import zarr
from dotenv import load_dotenv
from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import from_url
from virtualizarr import open_virtual_dataset
from virtualizarr.parsers import HDFParser

log = logging.getLogger("virtualize")


# --------------------------------------------------------------------------- #
# Configuration: everything that differs between datasets lives in datasets.yml;
# the Dataset dataclass below is its schema (field names, types, defaults).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Source:
    """One ranked stream of a "best time series" dataset (see Dataset.sources).
    file_regex must have the named groups date (YYYYMMDD) and hour (HH) of the
    model cycle, and lead (hours); a file is valid at
    cycle + lead + lead_offset_hours (e.g. -24 for WCOFS nowcasts n003..n024,
    which end at the cycle time)."""
    kind: str  # e.g. "nowcast", "forecast"
    file_regex: str
    lead_offset_hours: int = 0


@dataclass(frozen=True)
class Dataset:
    # Source: anonymous public object storage. Listing uses the bucket URL;
    # references embed the public HTTPS URL, which any reader can use.
    bucket_url: str  # e.g. "s3://noaa-nos-ofs-pds" (listing)
    http_base: str  # e.g. "https://noaa-nos-ofs-pds.s3.amazonaws.com/" (references)
    key_root: str  # prefix inside the bucket that holds the dataset, ends in "/"
    key_template: str  # per-day folder under key_root; strftime fields from the date
    concat_dim: str  # dimension the files are appended along
    # Which files in a day folder belong to this stream (one file = one step;
    # valid times are read from the files). Either this or `sources`.
    file_regex: str = ""
    # A "best time series" over several ranked streams, most preferred first
    # (e.g. nowcast, then forecast). Cycle, lead and valid time come from the
    # file names; each valid time is filled from the best file available, and
    # later runs replace a step whenever a better file appears: an earlier
    # source wins, then a later cycle. Each step's source is recorded in the
    # coordinates source_kind and forecast_reference_time.
    sources: tuple[Source, ...] = ()
    # Variables to materialize (small ones only!). The concat coordinate must be
    # included. Everything else stays a virtual reference.
    loadable: tuple[str, ...] = ()
    # Days of folders scanned on each side of --start/--end. The range selects
    # valid times, and a folder named for day D can hold data of neighbouring
    # days (WCOFS: D-1 06:00 .. D 03:00).
    folder_margin_days: int = 1
    time_chunk: int = 4096  # chunk length of the (materialized) concat coordinate
    # Global attributes naming one particular run/file (they would otherwise end
    # up describing whichever file was indexed last). Attribute names; each may
    # use shell-style wildcards (*, ?, [...]), e.g. "frc_file_*".
    drop_global_attrs: tuple[str, ...] = ("file", "history")
    # Extra coordinates that reuse another coordinate's references, as
    # {alias: source}, e.g. {lat: lat_rho, lon: lon_rho}. Viewers that only
    # recognise coordinates named lat/lon (gridlook) otherwise pick an arbitrary
    # one of several staggered grids. No data is copied.
    coord_aliases: dict[str, str] = dataclasses.field(default_factory=dict)
    # A regular lat/lon grid that the files store as 2-D coordinate arrays (NOAA
    # OFS "regulargrid" files), as {lat: <2-D name>, lon: <2-D name>}. The two
    # arrays (which must be in `loadable`) are checked to be rectilinear and
    # evenly spaced, then replaced by 1-D `lat`/`lon` coordinates, and their
    # dimensions are renamed to lat/lon (metadata only; references unchanged).
    rectilinear: dict[str, str] = dataclasses.field(default_factory=dict)
    # Multiscale overviews (see "Multiscale overviews" below), as
    # {variables: [...], levels: 3 | auto, method: mean|min|max|nearest,
    #  keep_mantissa_bits: 0..23 (optional dataset default, lossy float32),
    #  keep_mantissa_bits_by_variable: {name: 0..23 | null}}.
    # Present = built by default; --overviews/--no-overviews override.
    # Needs `rectilinear` (or 1-D lat/lon coordinates).
    overviews: dict | None = None
    # Reading the source (overview builds and validation fetch real bytes from
    # it). A burst of hundreds of new connections to one host can make connects
    # time out on home/NAT networks; these keep reads to a sane rate and retry.
    source_concurrency: int = 32  # concurrent HTTP requests (icechunk + zarr)
    source_connect_timeout: int = 30  # seconds per TCP/TLS connect (object_store default: 5)
    source_timeout: int = 300  # seconds per GET, whole request
    source_http_retries: int = 10  # tries per chunk GET, incl. the first (backoff 0.5 s .. 30 s)
    source_read_retries: int = 5  # retries of a whole failed read, 30 s * 2^i apart (max 5 min)

    @property
    def url_prefix(self) -> str:
        return self.http_base + self.key_root


DATASETS_FILE = Path(__file__).with_name("datasets.yml")
SOURCE_FIELDS = ("source_concurrency", "source_connect_timeout", "source_timeout",
                 "source_http_retries", "source_read_retries")


def parse_sources(raw, where: str) -> tuple[Source, ...]:
    if not isinstance(raw, list) or not raw:
        raise SystemExit(f"{where}: sources must be a non-empty list")
    out = []
    for i, src in enumerate(raw):
        if not isinstance(src, dict) or set(src) - {"kind", "file_regex", "lead_offset_hours"} \
                or not {"kind", "file_regex"} <= set(src):
            raise SystemExit(f"{where}: sources[{i}] must be a mapping with kind, file_regex "
                             "and optionally lead_offset_hours")
        try:
            groups = set(re.compile(src["file_regex"]).groupindex)
        except re.error as e:
            raise SystemExit(f"{where}: sources[{i}].file_regex: {e}")
        if not {"date", "hour", "lead"} <= groups:
            raise SystemExit(f"{where}: sources[{i}].file_regex needs the named groups "
                             "(?P<date>...), (?P<hour>...) and (?P<lead>...)")
        offset = src.get("lead_offset_hours", 0)
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise SystemExit(f"{where}: sources[{i}].lead_offset_hours must be an integer")
        out.append(Source(str(src["kind"]), src["file_regex"], offset))
    if len({s.kind for s in out}) != len(out):
        raise SystemExit(f"{where}: sources: kinds must be unique")
    return tuple(out)


def load_datasets(path: Path = DATASETS_FILE) -> dict[str, Dataset]:
    """Read dataset presets from YAML: {name: {Dataset field: value, ...}}."""
    raw = yaml.safe_load(path.read_text()) or {}
    out: dict[str, Dataset] = {}
    for name, fields in raw.items():
        fields = dict(fields or {})
        for key in ("loadable", "drop_global_attrs"):  # YAML lists -> tuples
            if key in fields:
                if not isinstance(fields[key], list):
                    raise SystemExit(f"{path}: dataset {name!r}: {key} must be a list")
                fields[key] = tuple(str(v) for v in fields[key])
        for key in ("coord_aliases", "rectilinear"):
            if key in fields:
                if not isinstance(fields[key], dict):
                    raise SystemExit(f"{path}: dataset {name!r}: {key} must be a mapping")
                fields[key] = {str(k): str(v) for k, v in fields[key].items()}
        if fields.get("rectilinear") and set(fields["rectilinear"]) != {"lat", "lon"}:
            raise SystemExit(f"{path}: dataset {name!r}: rectilinear needs exactly the keys lat and lon")
        if fields.get("file_regex") is None:  # null: unset an inherited (<<: *anchor) regex
            fields.pop("file_regex", None)
        if "sources" in fields:
            fields["sources"] = parse_sources(fields["sources"], f"{path}: dataset {name!r}")
        if bool(fields.get("file_regex")) == bool(fields.get("sources")):
            raise SystemExit(f"{path}: dataset {name!r}: set exactly one of file_regex and sources")
        for key in SOURCE_FIELDS:
            if key in fields and (isinstance(fields[key], bool) or not isinstance(fields[key], int)
                                  or fields[key] < (0 if key == "source_read_retries" else 1)):
                raise SystemExit(f"{path}: dataset {name!r}: {key} must be a positive integer")
        if "folder_margin_days" in fields and (isinstance(fields["folder_margin_days"], bool)
                                               or not isinstance(fields["folder_margin_days"], int)
                                               or fields["folder_margin_days"] < 0):
            raise SystemExit(f"{path}: dataset {name!r}: folder_margin_days must be an integer >= 0")
        if "overviews" in fields:
            ov = fields["overviews"]
            if ov is None:
                ov = {}
            if not isinstance(ov, dict) or set(ov) - {
                    "variables", "levels", "method", "keep_mantissa_bits", "keep_mantissa_bits_by_variable"}:
                raise SystemExit(f"{path}: dataset {name!r}: overviews must be a mapping with "
                                 "variables, levels, method, keep_mantissa_bits and/or "
                                 "keep_mantissa_bits_by_variable")
            if "variables" in ov and not isinstance(ov["variables"], list):
                raise SystemExit(f"{path}: dataset {name!r}: overviews.variables must be a list")
            parse_keep_mantissa_bits(ov.get("keep_mantissa_bits"))
            parse_variable_rounding(ov.get("keep_mantissa_bits_by_variable", {}))
            fields["overviews"] = ov
        try:
            out[name] = Dataset(**fields)
        except TypeError as e:  # unknown or missing field
            raise SystemExit(f"{path}: dataset {name!r}: {e}")
    return out


# --------------------------------------------------------------------------- #
# Source side: discovery and opening. No destination credentials involved.
# --------------------------------------------------------------------------- #
def anonymous_store(url: str):
    """obstore store for a public source URL.

    skip_signature = anonymous access; without it obstore goes looking for AWS
    credentials (including the instance-metadata service). obstore also reads
    AWS_* variables when it builds an S3 store, including for https://...
    amazonaws.com URLs, but here those variables describe the destination
    (AWS_ENDPOINT_URL may point at a local server), so they are hidden."""
    saved = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("AWS_")}
    try:
        return from_url(url, skip_signature=True)
    finally:
        os.environ.update(saved)


def source_store(ds: Dataset, bucket: bool = False):
    return anonymous_store(ds.bucket_url if bucket else ds.http_base)


@dataclass(frozen=True)
class TimeArg:
    """A --start/--end value: a UTC time, or a whole day (date given alone)."""
    when: dt.datetime  # naive, UTC
    date_only: bool

    def __str__(self) -> str:
        return self.when.date().isoformat() if self.date_only else self.when.isoformat()


def utc_today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def parse_time(s: str) -> TimeArg:
    """YYYY-MM-DD or an ISO datetime (YYYY-MM-DDTHH[:MM[:SS]], 'T' or space),
    always UTC. A Z/+00:00 suffix is allowed; other offsets are refused.
    'today' or 'today-N' is a whole day, N days before today (UTC)."""
    m = re.fullmatch(r"today(?:-(\d+))?", s)
    if m:
        day = utc_today() - dt.timedelta(days=int(m.group(1) or 0))
        return TimeArg(dt.datetime.combine(day, dt.time()), True)
    try:
        return TimeArg(dt.datetime.combine(dt.date.fromisoformat(s), dt.time()), True)
    except ValueError:
        pass
    try:
        t = dt.datetime.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"invalid time {s!r}; use YYYY-MM-DD or YYYY-MM-DDTHH[:MM[:SS]] (UTC)")
    if t.tzinfo is not None:
        if t.utcoffset() != dt.timedelta(0):
            raise argparse.ArgumentTypeError(f"{s!r}: times are UTC; drop the offset")
        t = t.replace(tzinfo=None)
    return TimeArg(t, False)


def time_bounds(start: TimeArg, end: TimeArg | None) -> tuple[np.datetime64, np.datetime64 | None, bool]:
    """(lower, upper, upper_inclusive). A date-only end covers its whole day,
    i.e. up to but not including 00:00 of the next day. No end: no upper bound."""
    lo = np.datetime64(start.when, "ns")
    if end is None:
        return lo, None, True
    if end.date_only:
        return lo, np.datetime64(end.when + dt.timedelta(days=1), "ns"), False
    return lo, np.datetime64(end.when, "ns"), True


def in_range(times: np.ndarray, start: TimeArg, end: TimeArg | None) -> np.ndarray:
    """Which of `times` (datetime64) fall in start..end, both inclusive."""
    lo, hi, incl = time_bounds(start, end)
    if hi is None:
        return times >= lo
    return (times >= lo) & ((times <= hi) if incl else (times < hi))


@dataclass(frozen=True)
class Candidate:
    """A source file. For a `sources` dataset, its stream, cycle and valid time
    come from its name; for a plain one they are unknown until it is opened."""
    url: str
    source: int = -1  # index into Dataset.sources; -1: plain file_regex dataset
    cycle: np.datetime64 | None = None
    valid: np.datetime64 | None = None

    @property
    def rank(self) -> tuple[int, int]:
        """Lower is better: an earlier source, then a later cycle."""
        return rank_of(self.source, self.cycle)

    @property
    def name(self) -> str:
        return self.url.rsplit("/", 1)[-1]


def rank_of(source: int, cycle: np.datetime64) -> tuple[int, int]:
    return int(source), -int(np.datetime64(cycle, "s").astype("int64"))


def classify(ds: Dataset, url: str, name: str) -> Candidate | None:
    """Candidate for a file name, or None if no stream of `ds` matches it."""
    if not ds.sources:
        return Candidate(url) if re.match(ds.file_regex, name) else None
    for i, src in enumerate(ds.sources):
        m = re.match(src.file_regex, name)
        if m:
            try:
                cycle = dt.datetime.strptime(m["date"] + m["hour"], "%Y%m%d%H")
            except ValueError:
                raise SystemExit(f"{name}: date/hour groups of the {src.kind} file_regex are not "
                                 "YYYYMMDD and HH")
            valid = cycle + dt.timedelta(hours=int(m["lead"]) + src.lead_offset_hours)
            return Candidate(url, i, np.datetime64(cycle, "ns"), np.datetime64(valid, "ns"))
    return None


def discover(ds: Dataset, start: TimeArg, end: TimeArg | None) -> list[Candidate]:
    """Return all files (sorted by URL) in the day folders that can hold data of
    start..end: the range plus folder_margin_days each side; without an end,
    through today (UTC). Plain datasets' files still have to be filtered by the
    valid time read from them (in_range); `sources` datasets' are filtered here."""
    store = source_store(ds, bucket=True)
    out: list[Candidate] = []
    empty_days: list[str] = []
    margin = dt.timedelta(days=ds.folder_margin_days)
    today = utc_today()
    core = (start.when.date(), end.when.date() if end else today)
    day, last = core[0] - margin, core[1] + margin
    while day <= last:
        prefix = ds.key_root + day.strftime(ds.key_template)
        found = [c for batch in store.list(prefix=prefix) for o in batch
                 if (c := classify(ds, ds.http_base + o["path"], o["path"].rsplit("/", 1)[-1]))]
        # Margin folders, and folders of days to come (or today's, before its
        # first cycle when there is no --end), may not exist yet.
        if not found and core[0] <= day <= core[1] and (day < today if end is None else day <= today):
            empty_days.append(day.isoformat())
        out += found
        day += dt.timedelta(days=1)
    if empty_days:
        # A build that quietly drops days is worse than one that says so.
        log.warning("no matching files for %d day(s): %s", len(empty_days), ", ".join(empty_days))
    if ds.sources:
        valid = np.array([c.valid for c in out], dtype="datetime64[ns]")
        out = [c for c, k in zip(out, in_range(valid, start, end)) if k]
    return sorted(out, key=lambda c: c.url)


def select_best(cands: list[Candidate]) -> list[Candidate]:
    """The best file for each valid time, in valid-time order."""
    best: dict[np.datetime64, Candidate] = {}
    for c in cands:
        if c.valid not in best or c.rank < best[c.valid].rank:
            best[c.valid] = c
    return [best[t] for t in sorted(best)]


def _open_one(args: tuple[str, str, tuple[str, ...]]) -> xr.Dataset:
    """Worker: open one file as a virtual dataset. Runs in a subprocess because
    h5py holds a global lock during I/O, so threads would not overlap requests."""
    url, http_base, loadable = args
    registry = ObjectStoreRegistry({http_base: anonymous_store(http_base)})
    return open_virtual_dataset(
        url, registry=registry, parser=HDFParser(), loadable_variables=list(loadable)
    )


def open_files(ds: Dataset, urls: list[str], workers: int) -> list[xr.Dataset]:
    args = [(u, ds.http_base, ds.loadable) for u in urls]
    if workers <= 1 or len(urls) == 1:
        return [_open_one(a) for a in args]
    # forkserver, not fork: by now this process runs threads (zarr's I/O loop, the
    # obstore/icechunk runtimes), and a forked child can inherit their locks held
    # and hang on its first request. (Python 3.14 makes forkserver the default.)
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("forkserver")) as pool:
        return list(pool.map(_open_one, args))


def fingerprint(vds: xr.Dataset, dim: str) -> dict:
    """What must agree between files for their arrays to be one Zarr array:
    names, dims, shape apart from the concat dim, dtype, and chunk shape."""
    out = {}
    for name, v in vds.variables.items():
        shape = tuple(n for d, n in zip(v.dims, v.shape) if d != dim)
        chunks = getattr(v.data, "chunks", None)
        out[name] = (v.dims, shape, str(v.dtype), tuple(chunks) if chunks else None)
    return out


# --------------------------------------------------------------------------- #
# Metadata repair: the one thing a virtual store can fix. Every change is logged.
# --------------------------------------------------------------------------- #
def _json_safe(value):
    """Return (value, changed) with NaN/Inf (not JSON-serializable) removed."""
    if isinstance(value, (float, np.floating)) and not math.isfinite(value):
        return None, True
    if isinstance(value, (list, tuple, np.ndarray)):
        arr = np.asarray(value)
        if arr.dtype.kind == "f" and not np.isfinite(arr).all():
            return None, True
    return value, False


def _decode_fill(value, dtype: np.dtype):
    """Numeric value of a _FillValue attribute, or None. Zarr v3 stores float
    fill values as base64 of the little-endian bytes (xarray's encoding), which
    only xarray decodes."""
    if isinstance(value, str):
        try:
            raw = base64.b64decode(value, validate=True)
        except ValueError:
            return None
        if dtype.kind != "f" or len(raw) not in (4, 8):
            return None
        value = np.frombuffer(raw, dtype=f"<f{len(raw)}")[0]
    if not isinstance(value, (int, float, np.number)) or not math.isfinite(value):
        return None
    return value


def to_rectilinear(vds: xr.Dataset, spec: dict[str, str]) -> xr.Dataset:
    """Replace 2-D lat/lon arrays that describe a regular grid by 1-D `lat`/`lon`
    dimension coordinates. Only dimension names change on the virtual arrays."""
    lat_name, lon_name = spec["lat"], spec["lon"]
    for n in (lat_name, lon_name):
        if n not in vds.variables:
            raise SystemExit(f"rectilinear: {n!r} not in the source files")
    lat2, lon2 = vds[lat_name], vds[lon_name]
    if lat2.ndim != 2 or lat2.dims != lon2.dims:
        raise SystemExit(f"rectilinear: {lat_name} and {lon_name} must be 2-D over the same dimensions")
    ydim, xdim = lat2.dims
    try:
        lat_v, lon_v = np.asarray(lat2.values, dtype="f8"), np.asarray(lon2.values, dtype="f8")
    except (TypeError, NotImplementedError):  # a virtual (unloaded) array has no values
        raise SystemExit(f"rectilinear: {lat_name} and {lon_name} must be in loadable")
    # Rows of constant latitude, columns of constant longitude, even spacing.
    if np.ptp(lat_v, axis=1).max() > 1e-9 or np.ptp(lon_v, axis=0).max() > 1e-9:
        raise SystemExit(f"rectilinear: {lat_name}/{lon_name} do not form a rectilinear grid "
                         "(curvilinear grids cannot be described this way)")
    lat, lon = lat_v[:, 0], lon_v[0, :]
    for name, c in (("lat", lat), ("lon", lon)):
        d = np.diff(c)
        if c.size < 2 or not np.allclose(d, d[0], rtol=1e-6, atol=0):
            raise SystemExit(f"rectilinear: {name} is not evenly spaced")
    for dim_name in ("lat", "lon"):
        if dim_name in vds.variables or dim_name in vds.dims:
            raise SystemExit(f"rectilinear: {dim_name!r} already exists in the source files")
    vds = vds.drop_vars([lat_name, lon_name]).rename_dims({ydim: "lat", xdim: "lon"})
    log.info("rectilinear: %s x %s grid, %.6g x %.6g degrees (%s, %s -> lat, lon)",
             lat.size, lon.size, lat[1] - lat[0], lon[1] - lon[0], ydim, xdim)
    return vds.assign_coords(
        lat=("lat", lat, {"standard_name": "latitude", "long_name": "latitude",
                          "units": "degrees_north", "axis": "Y"}),
        lon=("lon", lon, {"standard_name": "longitude", "long_name": "longitude",
                          "units": "degrees_east", "axis": "X"}),
    )


def repair_metadata(vds: xr.Dataset, dim: str, drop_global: tuple[str, ...],
                    coord_aliases: dict[str, str] | None = None) -> xr.Dataset:
    changes: list[str] = []
    # Plain-number missing_value next to the base64 _FillValue, so readers other
    # than xarray (e.g. gridlook) mask land/fill cells. Same value, so xarray's
    # CF decoding is unchanged.
    for name, v in vds.variables.items():
        if "missing_value" in v.attrs or "_FillValue" not in v.attrs:
            continue
        fill = _decode_fill(v.attrs["_FillValue"], v.dtype)
        if fill is not None:
            v.attrs["missing_value"] = np.asarray(fill, dtype=v.dtype).item()
            changes.append(f"{name}: added numeric missing_value")
    for alias, source in (coord_aliases or {}).items():
        if alias in vds.variables:
            raise SystemExit(f"coord_aliases: {alias!r} already exists in the source files")
        if source not in vds.variables:
            raise SystemExit(f"coord_aliases: source {source!r} not in the source files")
        src = vds[source].variable
        attrs = dict(src.attrs) | {"comment": f"alias of {source} (same data) for viewers that look for {alias!r}"}
        vds = vds.assign_coords({alias: xr.Variable(src.dims, src.data, attrs)})
        changes.append(f"{alias}: added alias of {source}")
    for key in [k for k in vds.attrs if any(fnmatch.fnmatchcase(k, pat) for pat in drop_global)]:
        del vds.attrs[key]
        changes.append(f"<global>: dropped run-specific attribute {key!r}")
    for scope, attrs in [("<global>", vds.attrs)] + [(n, v.attrs) for n, v in vds.variables.items()]:
        for key in list(attrs):
            safe, changed = _json_safe(attrs[key])
            if changed:
                del attrs[key]
                changes.append(f"{scope}: dropped non-finite attribute {key!r}")
    coord = vds[dim]
    # A decoded time coordinate gets CF's time markers, so catalogs and CF tools
    # (cf_xarray in Xreds) find it; the regulargrid files' time has neither.
    if coord.dtype.kind == "M" and "standard_name" not in coord.attrs:
        coord.attrs["standard_name"] = "time"
        changes.append(f"{dim}: added standard_name='time'")
    if coord.attrs.get("standard_name") == "time" and "axis" not in coord.attrs:
        coord.attrs["axis"] = "T"
        changes.append(f"{dim}: added axis='T'")
    if changes:
        log.info("metadata repair: %d change(s): %s", len(changes), "; ".join(sorted(set(c.split(": ", 1)[1] for c in changes))))
    return vds


# --------------------------------------------------------------------------- #
# Destination side: the Icechunk repository.
# --------------------------------------------------------------------------- #
def make_storage(repo_arg: str) -> ic.Storage:
    if repo_arg.startswith("s3://"):
        bucket, _, prefix = repo_arg[5:].partition("/")
        if not prefix:
            raise SystemExit("s3:// repositories need a non-empty prefix: s3://bucket/prefix")
        return ic.s3_storage(bucket=bucket, prefix=prefix, **s3_options())
    return ic.local_filesystem_storage(repo_arg)


def is_backblaze_endpoint(endpoint: str | None) -> bool:
    hostname = (urlsplit(endpoint).hostname or "").rstrip(".") if endpoint else ""
    return hostname.endswith(".backblazeb2.com")


def s3_options() -> dict:
    """ic.s3_storage keyword arguments from the standard AWS variables.

    AWS_ENDPOINT_URL selects an S3-compatible server (e.g. the local versitygw
    container); without it this is plain AWS. Explicit keys win; otherwise the
    AWS SDK's own lookup (profiles, SSO, instance metadata) is used."""
    env = lambda k: os.environ.get(k) or None  # noqa: E731 (empty counts as unset)
    endpoint = env("AWS_ENDPOINT_URL")
    if is_backblaze_endpoint(endpoint):
        # B2 includes checksum headers on empty 304 responses.
        os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "WHEN_REQUIRED")
    opts: dict = {"region": env("AWS_REGION") or env("AWS_DEFAULT_REGION")}
    if endpoint:
        opts |= {
            "endpoint_url": endpoint,
            "allow_http": endpoint.startswith("http://"),
            "force_path_style": True,  # no per-bucket DNS names on a local server
            "region": opts["region"] or "us-east-1",
        }
    key, secret = env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")
    if key and secret:
        opts |= {"access_key_id": key, "secret_access_key": secret, "session_token": env("AWS_SESSION_TOKEN")}
    else:
        opts["from_env"] = True
    return opts


def source_config(ds: Dataset) -> ic.RepositoryConfig:
    """Runtime-only settings for reading the source while building: bounded
    concurrency, patient connects, more retries. Never saved to the repository
    (readers keep the plain persisted container)."""
    store = ic.http_store({"connect_timeout": f"{ds.source_connect_timeout}s",
                           "timeout": f"{ds.source_timeout}s"})
    return ic.RepositoryConfig(
        max_concurrent_requests=ds.source_concurrency,
        storage=ic.StorageSettings(retries=ic.StorageRetriesSettings(
            max_tries=ds.source_http_retries, initial_backoff_ms=500, max_backoff_ms=30_000)),
        virtual_chunk_containers={ds.url_prefix: ic.VirtualChunkContainer(url_prefix=ds.url_prefix, store=store)},
    )


def read_source(fn, what: str, retries: int):
    """fn() (a read that fetches source bytes), retried after a pause when the
    source cannot be reached even after the per-request HTTP retries."""
    for attempt in range(retries + 1):
        try:
            return fn()
        except ic.StorageError as e:
            if "virtual reference" not in str(e) or attempt == retries:
                raise
            wait = min(300, 30 * 2**attempt)
            log.warning("source read of %s failed (%s); retry %d/%d in %ds", what,
                        str(e).splitlines()[0][:300], attempt + 1, retries, wait)
            time.sleep(wait)


def open_or_create_repo(
    storage: ic.Storage, ds: Dataset, *, unsafe_no_conditional_writes: bool = False,
) -> tuple[ic.Repository, bool]:
    """Create the repository if (and only if) it does not exist; any other error
    (auth, network, config) must surface, so there is no blanket except.
    The returned handle carries source_config (not persisted)."""
    auth = {ds.url_prefix: ic.credentials.HttpAccess}  # anonymous HTTP, but still explicit
    container = ic.VirtualChunkContainer(url_prefix=ds.url_prefix, store=ic.http_store())
    override = None
    if unsafe_no_conditional_writes:
        override = ic.RepositoryConfig(storage=ic.StorageSettings(
            unsafe_use_conditional_create=False, unsafe_use_conditional_update=False))
    created = not ic.Repository.exists(storage)
    if created:
        config = ic.RepositoryConfig.default()
        config.set_virtual_chunk_container(container)
        if override is not None:
            config = config.merge(override)
        repo = ic.Repository.create(storage, config=config, authorize_virtual_chunk_access=auth)
    else:
        repo = ic.Repository.open(storage, config=override, authorize_virtual_chunk_access=auth)
    needs_container = ds.url_prefix not in (repo.config.virtual_chunk_containers or {})
    if needs_container:
        config = repo.config
        config.set_virtual_chunk_container(container)
        repo = repo.reopen(config=config, authorize_virtual_chunk_access=auth)
    if created or override is not None or needs_container:
        # Persist destination compatibility settings and source containers.
        repo.save_config()
    settings = repo.config.storage
    if settings is not None and (settings.unsafe_use_conditional_create is False
                                or settings.unsafe_use_conditional_update is False):
        log.warning("conditional writes are disabled in this repository; allow only one writer "
                    "at a time, including builds and garbage collection")
    repo = repo.reopen(config=repo.config.merge(source_config(ds)), authorize_virtual_chunk_access=auth)
    return repo, created


def read_committed(repo: ic.Repository, dim: str) -> xr.Dataset | None:
    """Committed contents of main from a fresh read-only session, or None if the
    repository has no data yet."""
    store = repo.readonly_session("main").store
    try:
        zarr.open_group(store, mode="r")
    except zarr.errors.GroupNotFoundError:
        return None
    return xr.open_zarr(store, consolidated=False, chunks=None)


def indexed_files(repo: ic.Repository, ds: Dataset) -> set[str]:
    """Source URLs recorded in the commit metadata of main's history. Lets a re-run
    skip files without opening them. The time-coordinate check in build() remains
    the authority for repositories whose commits lack this record."""
    done: set[str] = set()
    for snap in repo.ancestry(branch="main"):
        done.update(ds.url_prefix + k for k in (snap.metadata or {}).get("files", []))
    return done


def versions() -> dict[str, str]:
    pkgs = ["icechunk", "virtualizarr", "xarray", "zarr", "obstore", "obspec-utils"]
    return {p: md.version(p) for p in pkgs} | {"python": sys.version.split()[0]}


def combine(vds_list: list[xr.Dataset], ds: Dataset) -> xr.Dataset:
    """Concatenate by hand. coords/compat/combine_attrs avoid comparing and
    broadcasting manifest-only arrays. Variables without the concat dim (the
    static grid) come from the first file."""
    vds = xr.concat(
        vds_list, dim=ds.concat_dim, data_vars="minimal", coords="minimal",
        compat="override", combine_attrs="drop_conflicts",
    )
    if ds.rectilinear:
        vds = to_rectilinear(vds, ds.rectilinear)
    return repair_metadata(vds, ds.concat_dim, ds.drop_global_attrs, ds.coord_aliases)


# --------------------------------------------------------------------------- #
# Best time series (`sources` datasets): which file fills each step, and when a
# newly available file should replace it.
# --------------------------------------------------------------------------- #
PROVENANCE = ("source_kind", "forecast_reference_time")


def kinds_of(ds: Dataset) -> str:
    return " ".join(src.kind for src in ds.sources)


def tag_provenance(vds: xr.Dataset, cand: Candidate, ds: Dataset) -> xr.Dataset:
    """Record the file's stream and cycle as (materialized) coordinates."""
    dim, n = ds.concat_dim, vds.sizes[ds.concat_dim]
    return vds.assign_coords(
        source_kind=(dim, np.full(n, cand.source, dtype="i1"), {
            "long_name": "source stream of each step (lower is preferred)",
            "flag_values": list(range(len(ds.sources))), "flag_meanings": kinds_of(ds)}),
        forecast_reference_time=(dim, np.full(n, cand.cycle, dtype="datetime64[ns]"), {
            "standard_name": "forecast_reference_time",
            "long_name": "model cycle that produced each step"}),
    )


def plan_best(best: list[Candidate], committed: xr.Dataset | None, have: np.ndarray,
              ds: Dataset, force: bool) -> tuple[list[Candidate], dict]:
    """Of the best files per valid time, those that need work: valid times not
    in the store yet (append), and ones the store holds from a worse file
    (replace; all of them with --force). Also returns {valid time: (source,
    cycle)} of the stored steps."""
    stored: dict = {}
    if committed is not None:
        if not set(PROVENANCE) <= set(committed.variables):
            raise SystemExit("this repository does not record source_kind/forecast_reference_time; "
                             "it was not built from `sources` (use a plain preset, or a new repository)")
        stored_kinds = committed["source_kind"].attrs.get("flag_meanings")
        if stored_kinds != kinds_of(ds):
            raise SystemExit(f"this repository ranks the sources {stored_kinds!r}, the preset "
                             f"{kinds_of(ds)!r}; the order must not change")
        stored = dict(zip(have, zip(committed["source_kind"].values,
                                    committed["forecast_reference_time"].values)))
    todo: list[Candidate] = []
    new, better, kept = [], [], []  # valid times of each category
    for c in best:
        old = stored.get(c.valid)
        if old is None:
            new.append(c.valid)
        elif force or c.rank < rank_of(*old):
            better.append(c.valid)
        else:
            kept.append(c.valid)
            continue
        todo.append(c)

    def span(t: list) -> str:
        return f" ({_short_times(np.array(t))})" if t else ""
    log.info("best files: %d new step(s)%s, %d to %s%s, %d already best%s",
             len(new), span(new), len(better), "re-ingest (--force)" if force else "upgrade",
             span(better), len(kept), span(kept))
    return todo, stored


def replace_steps(session: ic.Session, vds: xr.Dataset, dim: str, start: int) -> None:
    """Overwrite steps start..start+len of every time-dependent array with vds.
    The concat coordinate itself is not rewritten: the steps were matched by
    value, so it is already correct. Static variables and global attributes are
    left as they are."""
    group = zarr.open_group(session.store, mode="r+")
    coordinates = group.attrs.get("coordinates")
    static = [n for n, v in vds.variables.items() if dim not in v.dims]
    part = vds.drop_vars(static + [dim])
    part.attrs = {}
    part.vz.to_icechunk(session.store, region={dim: slice(start, start + part.sizes[dim])})
    if coordinates is not None:  # the writer resets it to the subset's coordinates
        group.update_attributes({"coordinates": coordinates})


# --------------------------------------------------------------------------- #
# Multiscale overviews (optional). A GeoZarr pyramid in the group "multiscale":
#   multiscale/        multiscales + proj: + spatial: attributes (via topozarr)
#   multiscale/0/      the overview variables again, as virtual references
#   multiscale/1..N/   materialized copies, each 2x coarser than the one before
# The root dataset is left exactly as without overviews, so readers that don't
# know about multiscales (gridlook, Xreds, plain xarray) see no difference.
# Level k is computed in memory from level k-1, so the source data is read once.
# --------------------------------------------------------------------------- #
OVERVIEW_GROUP = "multiscale"
OVERVIEW_METHODS = ("mean", "min", "max", "nearest")
OVERVIEW_BUDGET = 512 * 2**20  # bytes of one variable's level-0 data held at once
BITROUND_ALGORITHM = "float32-round-to-nearest-ties-to-even-v1"


@dataclass(frozen=True)
class Overviews:
    variables: tuple[str, ...]  # empty: every variable over (time, ..., lat, lon)
    levels: int | str = 3  # levels including level 0, or "auto" (topozarr's min_dim rule)
    method: str = "mean"
    keep_mantissa_bits: int | None = None
    keep_mantissa_bits_by_variable: dict[str, int | None] = dataclasses.field(default_factory=dict)

    def bits_for(self, variable: str) -> int | None:
        return self.keep_mantissa_bits_by_variable.get(variable, self.keep_mantissa_bits)


def parse_keep_mantissa_bits(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 23:
        raise SystemExit("overviews: keep_mantissa_bits must be an integer from 0 to 23")
    return value


def parse_variable_rounding(value) -> dict[str, int | None]:
    if not isinstance(value, dict) or any(not isinstance(k, str) or not k for k in value):
        raise SystemExit("overviews: keep_mantissa_bits_by_variable must map variable names to 0..23 or null")
    return {k: parse_keep_mantissa_bits(bits) for k, bits in value.items()}


def parse_variable_rounding_arg(value: str) -> tuple[str, int | None]:
    name, sep, raw = value.partition("=")
    try:
        if not name or not sep:
            raise ValueError
        bits = None if raw.lower() in ("none", "null") else int(raw)
        return name, parse_keep_mantissa_bits(bits)
    except (ValueError, SystemExit):
        raise argparse.ArgumentTypeError("expected VARIABLE=0..23 or VARIABLE=none") from None


def round_float32(value: np.ndarray, keep_mantissa_bits: int) -> np.ndarray:
    """Return a rounded copy, retaining float32 dtype and special-value bits.

    Based on dynamical-org/reformatters common/binary_rounding.py: round to
    nearest, ties to even. Work on a copy so the unrounded pyramid can continue.
    Unsigned addition carries into the exponent, including subnormal promotion
    and finite overflow to infinity. Exponent-all-ones values are never changed.
    """
    parse_keep_mantissa_bits(keep_mantissa_bits)
    if keep_mantissa_bits is None or value.dtype != np.dtype("float32"):
        raise ValueError("round_float32 requires float32 data and a bit count from 0 to 23")
    out = np.array(value, copy=True, order="C")
    if keep_mantissa_bits == 23:
        return out
    bits = out.view(np.uint32)
    finite = (bits & np.uint32(0x7f800000)) != np.uint32(0x7f800000)
    drop = 23 - keep_mantissa_bits
    selected = bits[finite]
    selected += ((selected >> drop) & np.uint32(1)) + np.uint32((1 << (drop - 1)) - 1)
    selected &= np.uint32(0xffffffff ^ ((1 << drop) - 1))
    bits[finite] = selected
    return out


def stored_rounding(group) -> tuple[int | None, dict[str, int | None]]:
    rounding = group.attrs.get("bitround")
    if rounding is None:
        return None, {}
    if not isinstance(rounding, dict) or rounding.get("algorithm") != BITROUND_ALGORITHM:
        raise SystemExit("overviews: unsupported stored bitround algorithm")
    if "keep_mantissa_bits" not in rounding:
        raise SystemExit("overviews: stored bitround metadata is missing keep_mantissa_bits")
    return (parse_keep_mantissa_bits(rounding["keep_mantissa_bits"]),
            parse_variable_rounding(rounding.get("keep_mantissa_bits_by_variable", {})))


def parse_levels(value) -> int | str:
    if value == "auto":
        return "auto"
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise SystemExit(f"overviews: levels must be an integer >= 2 or 'auto', not {value!r}")
    if n < 2:
        raise SystemExit("overviews: levels must be at least 2 (level 0 plus one overview)")
    return n


def stored_overviews(repo: ic.Repository, dim: str) -> Overviews | None:
    """The overview settings a repository was built with, or None."""
    store = repo.readonly_session("main").store
    try:
        group = zarr.open_group(store, path=OVERVIEW_GROUP, mode="r")
    except (zarr.errors.GroupNotFoundError, FileNotFoundError, KeyError):
        return None
    layout = group.attrs.get("multiscales", {}).get("layout", [])
    method = next((e.get("resampling_method") for e in layout[1:] if e.get("resampling_method")), "mean")
    level0 = zarr.open_group(store, path=f"{OVERVIEW_GROUP}/0", mode="r")
    variables = tuple(sorted(
        n for n, a in level0.arrays()
        if {dim, "lat", "lon"} <= set(a.metadata.dimension_names or ())))
    default, overrides = stored_rounding(group)
    return Overviews(variables, len(layout), method, default, overrides)


def resolve_overviews(args: argparse.Namespace, ds: Dataset, has_data: bool,
                      stored: Overviews | None) -> Overviews | None:
    """Decide whether this build maintains overviews. An existing repository
    decides by itself (its overviews must stay in step with the root); a new one
    follows the command line, else the preset."""
    requested = args.overviews
    if has_data:
        if stored is not None:
            if requested is False:
                raise SystemExit("this repository has overviews, which must be updated with every "
                                 "build; drop --no-overviews")
            if (args.overview_variables or args.overview_levels or args.overview_method
                    or args.overview_keep_mantissa_bits is not None
                    or args.overview_keep_mantissa_bits_by_variable):
                log.warning("overviews: settings are fixed when a repository is created; "
                            "keeping %s", stored)
            return stored
        if requested:
            raise SystemExit("this repository was built without overviews; build a new "
                             "repository to have them")
        if ds.overviews is not None:
            log.warning("overviews: the preset asks for overviews, but this repository was built "
                        "without them; continuing without")
        return None
    if not (requested if requested is not None else ds.overviews is not None):
        return None
    cfg = dict(ds.overviews or {})
    if args.overview_variables:
        cfg["variables"] = args.overview_variables.split(",")
    if args.overview_levels:
        cfg["levels"] = args.overview_levels
    if args.overview_method:
        cfg["method"] = args.overview_method
    if args.overview_keep_mantissa_bits is not None:
        cfg["keep_mantissa_bits"] = args.overview_keep_mantissa_bits
    overrides = parse_variable_rounding(cfg.get("keep_mantissa_bits_by_variable", {}))
    overrides.update(dict(args.overview_keep_mantissa_bits_by_variable or []))
    method = str(cfg.get("method", "mean"))
    if method not in OVERVIEW_METHODS:
        raise SystemExit(f"overviews: method must be one of {', '.join(OVERVIEW_METHODS)}")
    if not ds.rectilinear:
        raise SystemExit("overviews need a regular lat/lon grid (see `rectilinear`); this "
                         "dataset's grid is not declared regular. Curvilinear grids (e.g. the ROMS "
                         "`fields` files) cannot have overviews: use the `regulargrid` presets")
    return Overviews(tuple(str(v) for v in cfg.get("variables") or ()),
                     parse_levels(cfg.get("levels", 3)), method,
                     parse_keep_mantissa_bits(cfg.get("keep_mantissa_bits")), overrides)


def overview_variables(vds: xr.Dataset, ov: Overviews, dim: str) -> list[str]:
    spatial = {dim, "lat", "lon"}
    eligible = [n for n, v in vds.data_vars.items() if spatial <= set(v.dims)]
    if not ov.variables:
        variables = eligible
    else:
        variables = list(ov.variables)
    bad = [v for v in ov.variables if v not in eligible]
    if bad:
        raise SystemExit(f"overviews: {', '.join(bad)} not in the files or not over "
                         f"({dim}, lat, lon); candidates: {', '.join(eligible)}")
    unknown = set(ov.keep_mantissa_bits_by_variable) - set(eligible)
    if unknown:
        raise SystemExit(f"overviews: bitround overrides name ineligible variables: {', '.join(sorted(unknown))}")
    bad = [v for v in variables if ov.bits_for(v) is not None and vds[v].dtype != np.dtype("float32")]
    if bad:
        raise SystemExit(f"overviews: bitround requires float32 variables: {', '.join(bad)}")
    return variables


def reduce2(a: np.ndarray, method: str) -> np.ndarray:
    """2x2 block reduction over the last two axes, skipping NaN; trailing odd rows
    and columns are dropped (xarray coarsen boundary="trim", as topozarr does)."""
    ny, nx = a.shape[-2] // 2, a.shape[-1] // 2
    a = a[..., : 2 * ny, : 2 * nx]
    if method == "nearest":  # top-left cell of each window
        return np.ascontiguousarray(a[..., ::2, ::2])
    blocks = a.reshape(*a.shape[:-2], ny, 2, nx, 2)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN windows (land) stay NaN
        if method == "mean":
            out = np.nanmean(blocks, axis=(-3, -1), dtype="f8")
        else:
            out = (np.nanmin if method == "min" else np.nanmax)(blocks, axis=(-3, -1))
    return out.astype(a.dtype, copy=False)


def _level_group_attrs(store, level: int) -> dict:
    from topozarr.metadata import level_group_attrs
    root = zarr.open_group(store, path=OVERVIEW_GROUP, mode="r")
    return level_group_attrs(dict(root.attrs), level)


def _overview_plan(lvl0: xr.Dataset, variables: list[str], ov: Overviews, dim: str):
    """topozarr's pyramid plan: GeoZarr metadata, level coordinates and chunking.
    Built from a one-step template; topozarr reads no data for it."""
    import xproj  # noqa: F401  (.proj accessor)
    from topozarr import create_pyramid
    template = lvl0[variables].isel({dim: slice(0, 1)})
    template = template.drop_vars([c for c in template.coords if template[c].ndim > 1])
    template = template.proj.assign_crs(spatial_ref="EPSG:4326")
    return create_pyramid(template, levels=ov.levels, x_dim="lon", y_dim="lat", method=ov.method)


def _first_write_encoding(plan, level: int, lvl0: xr.Dataset, variables: list[str],
                          dim: str, time_chunk: int) -> dict:
    """topozarr's chunks/shards for a level, with one time step per chunk and shard
    (so appends never rewrite a shard), and level 0's dtype and fill value."""
    plan_enc = plan.encoding[f"/{level}"]
    enc: dict = {}
    for v in variables:
        e = {k: val for k, val in plan_enc.get(v, {}).items() if k in ("chunks", "shards", "compressors")}
        dims = lvl0[v].dims
        for key in ("chunks", "shards"):
            if e.get(key) is not None:
                e[key] = tuple(1 if d == dim else n for d, n in zip(dims, e[key]))
        src = lvl0[v].encoding
        e["dtype"] = src.get("dtype", lvl0[v].dtype)
        if src.get("_FillValue") is not None:
            e["_FillValue"] = src["_FillValue"]
            e["fill_value"] = src["_FillValue"]  # zarr metadata too, for non-CF readers
        if src.get("missing_value") is not None:
            e["missing_value"] = src["missing_value"]
        enc[v] = e
    t = lvl0[dim].encoding
    enc[dim] = {k: t[k] for k in ("units", "calendar", "dtype") if k in t} | {"chunks": (time_chunk,)}
    return enc


@dataclass
class Progress:
    """Steps of this run that go through the overviews, for the log. `total`
    starts as the number of files queued and shrinks as files turn out to be
    outside the range or already indexed (plain presets only learn their valid
    times on opening; for `sources` presets it is exact from the start)."""
    total: int
    done: int = 0


def _short_times(t: np.ndarray) -> str:
    """'2026-11-15T06:00' or '2026-11-15T06:00 .. 2026-11-15T12:00'."""
    first, last = str(t[0])[:16], str(t[-1])[:16]
    return first if first == last else f"{first} .. {last}"


def update_overviews(session: ic.Session, vds: xr.Dataset, ov: Overviews, dim: str,
                     start: int, *, create: bool = False, replace: bool = False,
                     time_chunk: int = 4096, read_retries: int = 5,
                     progress: Progress | None = None) -> None:
    """Write the time steps of `vds` (positions start.. in the root arrays) to the
    overview pyramid: references to multiscale/0, coarsened data to 1..N.
    create: first write of the repository; replace: rewrite existing steps (--force)."""
    t_start = time.time()
    store = session.store
    variables = overview_variables(vds, ov, dim)
    n = vds.sizes[dim]
    static = [name for name, v in vds.variables.items() if dim not in v.dims]

    # Level 0: the same references as the root arrays.
    part = vds[variables]
    part.attrs = {}
    if create:
        part.vz.to_icechunk(store, group=f"{OVERVIEW_GROUP}/0")
    elif replace:
        part.drop_vars([s for s in static if s in part.variables] + [dim]).vz.to_icechunk(
            store, group=f"{OVERVIEW_GROUP}/0", region={dim: slice(start, start + n)})
    else:
        part.drop_vars([s for s in static if s in part.variables]).vz.to_icechunk(
            store, group=f"{OVERVIEW_GROUP}/0", append_dim=dim)

    # Level 0 as a lazy, decoded dataset (reads the source through the session).
    lvl0 = xr.open_zarr(store, group=f"{OVERVIEW_GROUP}/0", consolidated=False, chunks=None)
    plan = None
    if create:
        plan = _overview_plan(lvl0, variables, ov, dim)
        zarr.open_group(store, path=OVERVIEW_GROUP, mode="r+").attrs.update(plan.attrs)
        if ov.keep_mantissa_bits is not None or ov.keep_mantissa_bits_by_variable:
            zarr.open_group(store, path=OVERVIEW_GROUP, mode="r+").attrs["bitround"] = {
                "algorithm": BITROUND_ALGORITHM, "keep_mantissa_bits": ov.keep_mantissa_bits,
                "keep_mantissa_bits_by_variable": ov.keep_mantissa_bits_by_variable}
    layout = zarr.open_group(store, path=OVERVIEW_GROUP, mode="r").attrs["multiscales"]["layout"]
    levels = len(layout)
    shapes = [tuple(e["spatial:shape"]) for e in layout]
    zarr.open_group(store, path=f"{OVERVIEW_GROUP}/0", mode="r+").attrs.update(_level_group_attrs(store, 0))

    # Steps per pass: as many as fit OVERVIEW_BUDGET for the largest variable.
    step_bytes = max(math.prod(s for d, s in lvl0[v].sizes.items() if d != dim) * lvl0[v].dtype.itemsize
                     for v in variables)
    per_pass = max(1, OVERVIEW_BUDGET // step_bytes)
    exists = [not create] * levels
    other_coords = {c: lvl0[c] for c in lvl0.coords if c not in (dim, "lat", "lon") and dim not in lvl0[c].dims}
    for p0 in range(start, start + n, per_pass):
        t_pass = time.time()
        p1 = min(start + n, p0 + per_pass)
        data: list[dict[str, np.ndarray]] = [{} for _ in range(levels)]
        for v in variables:
            a = read_source(lambda: lvl0[v].isel({dim: slice(p0, p1)}).values,  # the only source read
                            f"{v} steps {p0}..{p1 - 1}", read_retries)
            for k in range(1, levels):
                a = reduce2(a, ov.method)
                if a.shape[-2:] != shapes[k]:
                    raise SystemExit(f"overviews: level {k} of {v} is {a.shape[-2:]}, "
                                     f"metadata says {shapes[k]}")
                data[k][v] = a
            del a
        times = lvl0[dim].isel({dim: slice(p0, p1)})
        for k in range(1, levels):
            group = f"{OVERVIEW_GROUP}/{k}"
            out = xr.Dataset(
                {v: (lvl0[v].dims,
                     round_float32(data[k][v], ov.bits_for(v))
                     if ov.bits_for(v) is not None else data[k][v],
                     dict(lvl0[v].attrs)) for v in variables},
                coords={dim: times.variable})
            if not exists[k]:
                tmpl = plan.level_templates[k]
                out = out.assign_coords({c: tmpl[c].variable for c in ("lat", "lon")} | other_coords)
                out.to_zarr(store, group=group, mode="w-", zarr_format=3, consolidated=False,
                            encoding=_first_write_encoding(plan, k, lvl0, variables, dim, time_chunk))
                exists[k] = True
            elif replace:
                out.drop_vars(dim).to_zarr(store, group=group, region={dim: slice(p0, p1)},
                                           zarr_format=3, consolidated=False)
            else:
                out.to_zarr(store, group=group, append_dim=dim, zarr_format=3, consolidated=False)
            # to_zarr replaces a group's attributes; restore the level's proj:/spatial:.
            zarr.open_group(store, path=group, mode="r+").attrs.update(_level_group_attrs(store, k))
        del data
        if progress is not None:  # 1-based position in this run
            k0, k1 = progress.done + p0 - start + 1, progress.done + p1 - start
            steps = f"{k0}..{k1}/{progress.total}" if k1 > k0 else f"{k0}/{progress.total}"
        else:
            steps = f"{p0}..{p1 - 1}"
        log.info("overviews: %s step%s %s (%s) of %s -> levels 1..%d, %.0fs",
                 "rewrote" if replace else "wrote", "s" if p1 - p0 > 1 else "", steps,
                 _short_times(times.values), ",".join(variables), levels - 1, time.time() - t_pass)
    if progress is not None:
        progress.done += n
    if n > per_pass:
        log.info("overviews: %d step(s) in %.0fs", n, time.time() - t_start)


def validate_overviews(store, dim: str, root_times: np.ndarray, read_retries: int = 5) -> None:
    """Check shapes/times and the last step's reduction chain. With bitround,
    reconstruct unrounded intermediates from level 0, rounding only outputs."""
    root = zarr.open_group(store, path=OVERVIEW_GROUP, mode="r")
    default_bits, overrides = stored_rounding(root)
    layout = root.attrs["multiscales"]["layout"]
    method = next((e.get("resampling_method") for e in layout[1:] if e.get("resampling_method")), "mean")
    levels = [xr.open_zarr(store, group=f"{OVERVIEW_GROUP}/{e['asset']}", consolidated=False, chunks=None)
              for e in layout]
    for k, (entry, lv) in enumerate(zip(layout, levels)):
        assert (lv[dim].values == root_times).all(), f"overview level {k}: time steps differ from the root"
        assert (lv.sizes["lat"], lv.sizes["lon"]) == tuple(entry["spatial:shape"]), \
            f"overview level {k}: shape differs from its metadata"
    # Mixed policies are checked for every variable; otherwise keep the existing
    # inexpensive check of the smallest variable, at one depth and the last time.
    variables = (list(levels[1].data_vars) if overrides else
                 [min(levels[1].data_vars, key=lambda v: math.prod(levels[1][v].shape[1:]))])
    for var in variables:
        keep_bits = overrides.get(var, default_bits)
        sel = {d: 0 for d in levels[0][var].dims if d not in (dim, "lat", "lon")} | {dim: -1}
        prev = read_source(lambda: levels[0][var].isel(sel).values, f"{var} (level 0)", read_retries)
        for k in range(1, len(levels)):
            got = read_source(lambda: levels[k][var].isel(sel).values, f"{var} (level {k})", read_retries)
            expected = reduce2(prev, method)
            rounded = round_float32(expected, keep_bits) if keep_bits is not None else expected
            assert (np.array_equal(rounded, got, equal_nan=True) if keep_bits is not None else
                    np.allclose(rounded, got, equal_nan=True, rtol=1e-5, atol=1e-6)), \
                f"overview level {k} of {var} is not the {method} of level {k - 1}"
            prev = expected if keep_bits is not None else got
    log.info("validated overviews: %d levels (%s), %s at the last step matches level by level",
             len(levels), " / ".join(f"{lv.sizes['lat']}x{lv.sizes['lon']}" for lv in levels), ",".join(variables))


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def build(args: argparse.Namespace, ds: Dataset, name: str) -> None:
    dim = ds.concat_dim
    zarr.config.set({"async.concurrency": ds.source_concurrency})

    cands = discover(ds, args.start, args.end)
    log.info("discovered %d candidate files for %s .. %s (±%d day(s) of folders)",
             len(cands), args.start, args.end or "open end", ds.folder_margin_days)
    if not cands:
        raise SystemExit("nothing to index")
    urls = [c.url for c in cands]
    best: list[Candidate] = []
    if ds.sources:
        best = select_best(cands)
        counts = {src.kind: sum(c.source == i for c in best) for i, src in enumerate(ds.sources)}
        log.info("%d valid time(s), %s; best files: %s", len(best),
                 _short_times(np.array([best[0].valid, best[-1].valid])),
                 ", ".join(f"{n} {k}" for k, n in counts.items()))
    if args.dry_run and ds.sources:
        # Valid times come from the names; nothing needs opening.
        for c in best:
            print(f"{c.url}\t{c.valid}\t{ds.sources[c.source].kind}\tcycle {c.cycle}")
        return
    if args.dry_run:
        # Valid times are only known from the files themselves.
        n = 0
        for i in range(0, len(urls), args.batch_size):
            batch_urls = urls[i : i + args.batch_size]
            vds_list = open_files(ds, batch_urls, args.workers)
            times = np.array([v[dim].values[0] for v in vds_list], dtype="datetime64[ns]")
            for u, t in zip(batch_urls, times):
                if in_range(np.array([t]), args.start, args.end)[0]:
                    print(f"{u}\t{t}")
                    n += 1
        log.info("%d file(s) in %s .. %s", n, args.start, args.end)
        return

    storage = make_storage(args.repo)
    disable_conditional_writes = args.unsafe_no_conditional_writes or (
        args.repo.startswith("s3://") and is_backblaze_endpoint(os.environ.get("AWS_ENDPOINT_URL")))
    repo, created = open_or_create_repo(
        storage, ds, unsafe_no_conditional_writes=disable_conditional_writes)
    log.info("%s repository %s", "created" if created else "opened", args.repo)

    committed = read_committed(repo, dim)
    if committed is not None:
        if committed.attrs.get("virtualized_dataset") != name:
            raise SystemExit(
                f"repository holds dataset {committed.attrs.get('virtualized_dataset')!r}, "
                f"not {name!r}; refusing to mix datasets"
            )
        have = committed[dim].values
        log.info("store already holds %d steps, %s .. %s", have.size, have[0], have[-1])
    else:
        have = np.array([], dtype="datetime64[ns]")
    ov = resolve_overviews(args, ds, committed is not None,
                           stored_overviews(repo, dim) if committed is not None else None)
    if ov:
        log.info("overviews: %s levels (%s), variables: %s", ov.levels, ov.method,
                 ", ".join(ov.variables) or "all over (time, lat, lon)")
        if ov.keep_mantissa_bits is not None:
            log.info("overviews: float32 bitround retaining %d mantissa bits", ov.keep_mantissa_bits)
        if ov.keep_mantissa_bits_by_variable:
            log.info("overviews: bitround variable overrides: %s", ov.keep_mantissa_bits_by_variable)

    stored: dict = {}
    if ds.sources:
        todo, stored = plan_best(best, committed, have, ds, args.force)
        urls = [c.url for c in todo]  # in valid-time order
        by_url = {c.url: c for c in todo}
    elif not args.force:
        already = indexed_files(repo, ds)
        pending = [u for u in urls if u not in already]
        if len(pending) < len(urls):
            log.info("skipping %d file(s) recorded as already indexed", len(urls) - len(pending))
        urls = pending

    total_new = total_replaced = in_window = commits = 0
    progress = Progress(total=len(urls))
    for i in range(0, len(urls), args.batch_size):
        batch_urls = urls[i : i + args.batch_size]
        vds_list = open_files(ds, batch_urls, args.workers)
        times = np.array([v[dim].values[0] for v in vds_list], dtype="datetime64[ns]")
        if ds.sources:
            cs = [by_url[u] for u in batch_urls]
            for c, t in zip(cs, times):
                if t != c.valid:
                    raise SystemExit(
                        f"{c.name} holds {t}, but its name says {c.valid}; check the "
                        f"{ds.sources[c.source].kind} source's file_regex/lead_offset_hours")
            vds_list = [tag_provenance(v, c, ds) for v, c in zip(vds_list, cs)]

        # Margin folders hold files outside the requested time range; drop them.
        inside = in_range(times, args.start, args.end)
        if not inside.all():
            log.info("batch %d: ignoring %d file(s) outside %s .. %s",
                     i // args.batch_size, int((~inside).sum()), args.start, args.end)
        vds_list = [v for v, k in zip(vds_list, inside) if k]
        batch_urls = [u for u, k in zip(batch_urls, inside) if k]
        times = times[inside]
        progress.total -= int((~inside).sum())
        in_window += len(vds_list)
        if not vds_list:
            continue

        # Reconcile against committed state: files whose valid time is already in
        # the store were indexed by an earlier run. Skip them, unless --force,
        # in which case their references are rewritten in place below.
        # (`sources` datasets: plan_best already chose what needs work.)
        keep = ~np.isin(times, have) | args.force | bool(ds.sources)
        if not keep.all():
            log.info("batch %d: skipping %d already-indexed file(s)", i // args.batch_size, int((~keep).sum()))
        vds_list = [v for v, k in zip(vds_list, keep) if k]
        batch_urls = [u for u, k in zip(batch_urls, keep) if k]
        times = times[keep]
        progress.total -= int((~keep).sum())
        if not vds_list:
            continue

        # Ordered, unique, and strictly after what is already committed.
        order = np.argsort(times)
        vds_list = [vds_list[j] for j in order]
        batch_urls = [batch_urls[j] for j in order]
        times = times[order]
        if len(np.unique(times)) != len(times):
            raise SystemExit("two files in this batch have the same valid time; ambiguous, stopping")
        replace = np.isin(times, have)  # only possible with --force
        new = ~replace
        if have.size and new.any() and times[new][0] <= have[-1]:
            j = int(np.argmax(new))
            raise SystemExit(
                f"{batch_urls[j]} is valid at {times[j]}, not after the last committed step "
                f"{have[-1]}; appending out of order is not supported (rebuild instead)"
            )

        # Homogeneity: refuse to combine files that would not form one array.
        # (Replacements are also checked against the stored arrays by the writer.)
        ref = fingerprint(vds_list[0], dim)
        for u, v in zip(batch_urls, vds_list):
            if fingerprint(v, dim) != ref:
                raise SystemExit(f"schema/chunk drift in {u}; partition the dataset instead")

        session = repo.writable_session("main")
        if replace.any():
            # Rewrite the references of steps already in the store, one region
            # per run of consecutive time indices.
            idx = np.searchsorted(have, times[replace])
            pos = np.flatnonzero(replace)
            for run in np.split(np.arange(idx.size), np.flatnonzero(np.diff(idx) != 1) + 1):
                part = combine([vds_list[pos[k]] for k in run], ds)
                replace_steps(session, part, dim, start=int(idx[run[0]]))
                if ov:
                    update_overviews(session, part, ov, dim, int(idx[run[0]]), replace=True,
                                     read_retries=ds.source_read_retries, progress=progress)
            log.info("batch %d: replacing %d already-indexed step(s)", i // args.batch_size, int(replace.sum()))
        if new.any():
            vds = combine([v for v, n in zip(vds_list, new) if n], ds)
            if have.size == 0:  # first write creates the arrays
                vds.attrs["virtualized_dataset"] = name
                vds.attrs["virtualized_source"] = ds.url_prefix
                # The concat coordinate is materialized; one big chunk instead of one
                # per time step keeps metadata reads fast. (Virtual arrays keep their
                # source chunking; nothing here can change that.)
                vds[dim].encoding["chunks"] = (ds.time_chunk,)
                vds[dim].encoding.pop("preferred_chunks", None)
                if ds.sources:
                    vds["source_kind"].encoding["chunks"] = (ds.time_chunk,)
                    vds["forecast_reference_time"].encoding.update(
                        chunks=(ds.time_chunk,), units="seconds since 1970-01-01T00:00:00", dtype="int64")
                vds.vz.to_icechunk(session.store)
                if ov:
                    update_overviews(session, vds, ov, dim, 0, create=True, time_chunk=ds.time_chunk,
                                     read_retries=ds.source_read_retries, progress=progress)
            else:
                static = [n for n, v in vds.variables.items() if dim not in v.dims]
                appended = vds.drop_vars(static)
                appended.attrs = {}  # appending would otherwise overwrite the global attrs
                appended.vz.to_icechunk(session.store, append_dim=dim)
                if ov:
                    update_overviews(session, vds, ov, dim, int(have.size), time_chunk=ds.time_chunk,
                                     read_retries=ds.source_read_retries, progress=progress)
        n_new, n_replaced = int(new.sum()), int(replace.sum())
        verb = "index" if not n_replaced else "re-index" if not n_new else "index/re-index"
        upgraded = []
        if ds.sources:  # what each replaced step held before
            for u, t, r in zip(batch_urls, times, replace):
                if r:
                    kind, cycle = stored[t]
                    upgraded.append({"file": u.removeprefix(ds.url_prefix),
                                     "from_kind": ds.sources[int(kind)].kind, "from_cycle": str(cycle)})
        snapshot = session.commit(
            f"{verb} {len(vds_list)} files: {batch_urls[0].rsplit('/', 1)[-1]} .. {batch_urls[-1].rsplit('/', 1)[-1]}",
            metadata={
                "dataset": name,
                "files": [u.removeprefix(ds.url_prefix) for u in batch_urls],
                "replaced": [u.removeprefix(ds.url_prefix) for u, r in zip(batch_urls, replace) if r],
                "first_time": str(times[0]),
                "last_time": str(times[-1]),
                "overviews": ov.levels if ov else None,
                **({"upgraded": upgraded} if ds.sources else {}),
                "versions": versions(),
            },
        )  # a committed session is read-only; the next batch opens a fresh one
        have = np.concatenate([have, times[new]])
        commits += 1
        total_new += n_new
        total_replaced += n_replaced
        log.info("committed %s: +%d new, %d replaced step(s) (%s .. %s)",
                 snapshot, n_new, n_replaced, times[0], times[-1])

    if urls and not in_window:
        log.warning("none of the opened files is valid in %s .. %s", args.start, args.end)
    if not urls:
        log.info("up to date: nothing to index or upgrade")
    log.info("done: %d new, %d replaced step(s), %d total", total_new, total_replaced, have.size)
    if commits and args.retain is not None:
        expire_and_gc(repo, args.retain)
    if not args.no_validate:
        validate(storage, ds, expected_steps=have.size, expected_last=have[-1] if have.size else None)
    if not args.no_catalog:
        root, repo_name = split_repo(args.repo)
        target = catalog_target(root, args.public_base_url, args.xreds_base_url, args.xreds_endpoint_url)
        upsert_catalogs(target, repo_name, storage)


def parse_duration(s: str) -> dt.timedelta:
    """'3d', '12h', '30m' or '0'."""
    m = re.fullmatch(r"(\d+)\s*([dhm]?)", s.strip())
    if not m or (not m.group(2) and m.group(1) != "0"):
        raise argparse.ArgumentTypeError(f"invalid duration {s!r}; use e.g. 3d, 12h, 30m or 0")
    unit = {"d": "days", "h": "hours", "m": "minutes", "": "days"}[m.group(2)]
    return dt.timedelta(**{unit: int(m.group(1))})


def expire_and_gc(repo: ic.Repository, retain: dt.timedelta) -> None:
    """Drop snapshots older than `retain` from history (main's tip is always
    kept) and delete the chunks, manifests, ... that only they used. Replacing
    steps (forecast upgrades, --force) otherwise keeps every superseded overview
    chunk forever. The cutoff is now, after this run's commits, so nothing this
    run wrote is at risk; with a retention of 0, no other writer may be
    committing to the repository at the same time."""
    t0 = time.time()
    cutoff = dt.datetime.now(dt.timezone.utc) - retain
    expired = repo.expire_snapshots(older_than=cutoff)
    gc = repo.garbage_collect(delete_object_older_than=cutoff)
    log.info("gc: expired %d snapshot(s) older than %s; deleted %d chunk(s) (%.1f MB), %d manifest(s), "
             "%d snapshot file(s) in %.1fs", len(expired), cutoff.isoformat(timespec="seconds"),
             gc.chunks_deleted, gc.bytes_deleted / 1e6, gc.manifests_deleted, gc.snapshots_deleted,
             time.time() - t0)
    if gc.objects_failed_to_delete:
        log.warning("gc: %d object(s) could not be deleted", gc.objects_failed_to_delete)


def validate(storage: ic.Storage, ds: Dataset, expected_steps: int, expected_last) -> None:
    """Reopen the way a consumer would (fresh repository object, read-only
    session, only the persisted config + explicit authorization) and read real
    bytes from the source, not just metadata."""
    repo = ic.Repository.open(storage, authorize_virtual_chunk_access={ds.url_prefix: ic.credentials.HttpAccess})
    store = repo.readonly_session("main").store
    out = xr.open_zarr(store, consolidated=False, chunks=None)
    t = out[ds.concat_dim].values
    assert t.size == expected_steps, f"expected {expected_steps} steps, store has {t.size}"
    assert (np.diff(t) > np.timedelta64(0, "ns")).all(), "time coordinate not strictly increasing"
    assert expected_last is None or t[-1] == expected_last
    var = next(n for n, v in out.data_vars.items() if ds.concat_dim in v.dims and v.ndim >= 3)
    da = out[var]
    window = {d: slice(da.sizes[d] // 2, da.sizes[d] // 2 + 4) for d in da.dims if d != ds.concat_dim}
    sample = read_source(lambda: da.isel(window).isel({ds.concat_dim: -1}).load(),  # fetches a source chunk
                         var, ds.source_read_retries)
    log.info(
        "validated: %d steps, %s .. %s; read %s%s from the source (dtype %s, %d finite of %d)",
        t.size, t[0], t[-1], var, tuple(sample.shape), sample.dtype,
        int(np.isfinite(sample.values).sum()), sample.size,
    )
    if ds.sources:
        for c in PROVENANCE:
            assert c in out.coords and out[c].dims == (ds.concat_dim,), f"{c} missing or not over {ds.concat_dim}"
        kind = out["source_kind"].values
        if (np.diff(kind.astype(int)) < 0).any():  # e.g. a missing nowcast cycle, later filled
            log.warning("validated: a %s step comes after a better one; some steps await an upgrade",
                        ds.sources[int(kind.max())].kind)
        log.info("validated provenance: %s", ", ".join(
            f"{src.kind} {int((kind == i).sum())} step(s)" for i, src in enumerate(ds.sources)))
    if stored_overviews(repo, ds.concat_dim) is not None:
        validate_overviews(store, ds.concat_dim, t, ds.source_read_retries)


# --------------------------------------------------------------------------- #
# Catalogs: gridlook.json and xreds.yml in _catalogs/ under the
# repository root (the directory/prefix holding the *.icechunk repositories). Entries are built
# from each repository's own metadata, so no preset is needed to describe one.
# --------------------------------------------------------------------------- #
CATALOG_DIR = "_catalogs"
GRIDLOOK_CATALOG = f"{CATALOG_DIR}/gridlook.json"
XREDS_CATALOG = f"{CATALOG_DIR}/xreds.yml"
# Defaults for a local root: the compose layout, where ./repos is the bucket
# "icechunk", served at localhost:7070 (browser) and s3:7070 (other containers).
LOCAL_PUBLIC_BASE = "http://localhost:7070/icechunk/"
LOCAL_XREDS_BASE = "s3://icechunk/"
COMPOSE_S3_ENDPOINT = "http://s3:7070"


@dataclass(frozen=True)
class CatalogTarget:
    root: str  # local directory or s3://bucket[/prefix], no trailing "/"
    public_base: str  # browser URL of the root, ends in "/"
    xreds_base: str  # s3:// URL of the root as Xreds sees it, ends in "/"
    xreds_endpoint: str | None  # S3 endpoint Xreds uses; None = AWS


def split_repo(repo_arg: str) -> tuple[str, str]:
    """(root, name) of a repository argument: its parent and its last component."""
    repo_arg = repo_arg.rstrip("/")
    if repo_arg.startswith("s3://"):
        root, _, name = repo_arg.rpartition("/")
        return root, name
    path = Path(repo_arg).resolve()
    return str(path.parent), path.name


def catalog_target(root: str, public_base: str | None, xreds_base: str | None,
                   xreds_endpoint: str | None) -> CatalogTarget:
    """Fill in defaults. An s3:// root is addressed through AWS_ENDPOINT_URL (or
    AWS itself); a local root is assumed to be ./repos of the compose setup."""
    root = root.rstrip("/")
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or None
    if root.startswith("s3://"):
        bucket, _, prefix = root[5:].partition("/")
        path = f"{prefix}/" if prefix else ""
        default_public = (f"{endpoint.rstrip('/')}/{bucket}/{path}" if endpoint
                          else f"https://{bucket}.s3.amazonaws.com/{path}")
        default_xreds, default_endpoint = root + "/", COMPOSE_S3_ENDPOINT if endpoint else None
    else:
        default_public, default_xreds, default_endpoint = LOCAL_PUBLIC_BASE, LOCAL_XREDS_BASE, COMPOSE_S3_ENDPOINT
    slash = lambda u: u if u.endswith("/") else u + "/"  # noqa: E731
    return CatalogTarget(
        root=root,
        public_base=slash(public_base or default_public),
        xreds_base=slash(xreds_base or default_xreds),
        xreds_endpoint=default_endpoint if xreds_endpoint is None else (xreds_endpoint or None),
    )


def root_store(root: str):
    """obstore store for an s3:// root, with the same AWS_* settings as s3_options()."""
    from obstore.store import S3Store
    env = lambda k: os.environ.get(k) or None  # noqa: E731
    bucket, _, prefix = root[5:].partition("/")
    endpoint = env("AWS_ENDPOINT_URL")
    kwargs: dict = {"region": env("AWS_REGION") or env("AWS_DEFAULT_REGION") or "us-east-1"}
    if endpoint:
        kwargs["endpoint"] = endpoint
    key, secret = env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")
    if key and secret:
        kwargs |= {"access_key_id": key, "secret_access_key": secret}
        if env("AWS_SESSION_TOKEN"):
            kwargs["session_token"] = env("AWS_SESSION_TOKEN")
    client = {"allow_http": True} if endpoint and endpoint.startswith("http://") else None
    return S3Store(bucket, prefix=prefix or None, client_options=client, **kwargs)


def read_root_file(root: str, name: str) -> str | None:
    if root.startswith("s3://"):
        import obstore
        from obstore.exceptions import NotFoundError
        try:
            return bytes(obstore.get(root_store(root), name).bytes()).decode()
        except (FileNotFoundError, NotFoundError):  # obstore raises either
            return None
    path = Path(root, name)
    return path.read_text() if path.is_file() else None


def write_root_file(root: str, name: str, text: str, content_type: str) -> None:
    if root.startswith("s3://"):
        import obstore
        obstore.put(root_store(root), name, text.encode(), attributes={"Content-Type": content_type})
    else:
        path = Path(root, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def list_repos(root: str) -> list[str]:
    """Names of the *.icechunk repositories directly under root."""
    if root.startswith("s3://"):
        names = [p.rstrip("/").rsplit("/", 1)[-1] for p in root_store(root).list_with_delimiter()["common_prefixes"]]
    else:
        names = [p.name for p in Path(root).iterdir() if p.is_dir()]
    return sorted(n for n in names if n.endswith(".icechunk"))


def describe_repo(storage: ic.Storage) -> dict | None:
    """Catalog-relevant facts about a repository, from its config and committed
    metadata only (no virtual chunks are read). None if it holds no data."""
    repo = ic.Repository.open(storage)
    prefixes = list((repo.config.virtual_chunk_containers or {}).keys())
    store = repo.readonly_session("main").store
    try:
        group = zarr.open_group(store, mode="r")
    except zarr.errors.GroupNotFoundError:
        return None
    attrs = dict(group.attrs)
    arrays = dict(group.arrays())
    variables = {name: dict(arr.attrs) for name, arr in arrays.items()}
    dims = {d for arr in arrays.values() for d in (arr.metadata.dimension_names or ()) if d}
    # Time range from the materialized time coordinate (axis T / standard_name time).
    t0 = t1 = None
    steps = 0
    by_kind: dict[str, str] = {}  # `sources` stores: last valid time of each stream
    for name, arr in arrays.items():
        a = arr.attrs
        if arr.ndim == 1 and (a.get("axis") == "T" or a.get("standard_name") == "time"):
            # Decode just this (materialized) array; opening the whole dataset
            # would read virtual scalars, which needs chunk authorization.
            var = xr.Variable(("t",), arr[:], {k: v for k, v in a.items() if k != "_ARRAY_DIMENSIONS"})
            t = xr.decode_cf(xr.Dataset({"t": var}))["t"].values
            if t.size:
                steps, t0, t1 = t.size, str(t[0])[:16], str(t[-1])[:16]
                if "source_kind" in arrays and arrays["source_kind"].shape == t.shape:
                    kind = arrays["source_kind"][:]
                    names = str(arrays["source_kind"].attrs.get("flag_meanings", "")).split()
                    by_kind = {k: str(t[kind == i][-1])[:16] for i, k in enumerate(names) if (kind == i).any()}
            break
    try:
        layout = group[OVERVIEW_GROUP].attrs.get("multiscales", {}).get("layout", [])
    except KeyError:
        layout = []
    return {"attrs": attrs, "prefix": attrs.get("virtualized_source") or (prefixes[0] if prefixes else None),
            "steps": steps, "first": t0, "last": t1, "by_kind": by_kind, "variables": variables, "dims": dims,
            "overview_levels": len(layout)}


# What Xreds' "roms" extension reads: it rotates u/v (on the u/v grids) onto the
# rho grid with `angle`, shaped like temp or salt (which need an s_rho dim).
ROMS_VARIABLES = {"angle", "lat_rho", "lon_rho", "u", "v"}
ROMS_DIMS = {"s_rho", "xi_rho", "eta_rho", "xi_u", "eta_u", "xi_v", "eta_v"}


def is_roms(info: dict) -> bool:
    names = info["variables"].keys()
    return (ROMS_VARIABLES <= names and ROMS_DIMS <= info["dims"]
            and bool({"temp", "salt"} & names))


def coord_aliases_in(info: dict) -> list[str]:
    """Coordinates added by coord_aliases (see repair_metadata), recognised by
    their comment. They duplicate their source's CF attributes, which makes
    cf_xarray (Xreds WMS/EDR) see two candidates for one coordinate."""
    return sorted(n for n, a in info["variables"].items()
                  if str(a.get("comment", "")).startswith("alias of "))


def catalog_entries(target: CatalogTarget, name: str, info: dict) -> tuple[dict, tuple[str, dict]]:
    """(gridlook entry, (xreds id, xreds entry)) for repository `name`."""
    attrs = info["attrs"]
    dataset_id = name.removesuffix(".icechunk")
    source = attrs.get("virtualized_dataset") or dataset_id
    parts = [str(attrs["title"]).strip()] if attrs.get("title") else []
    if info["first"]:
        parts.append(f"{info['steps']} time steps, {info['first']} to {info['last']}")
    if info.get("by_kind"):
        parts.append("best time series: " + ", ".join(f"{k} to {t}" for k, t in info["by_kind"].items()))
    if info["prefix"]:
        parts.append(f"Virtual Icechunk store over {info['prefix']}")
    gridlook = {"title": source, "url": target.public_base + name, "description": ". ".join(parts),
                "tags": {"repository": dataset_id, "format": "virtual-icechunk"}}
    if info["prefix"]:
        gridlook["tags"]["source"] = info["prefix"].split("/")[2]  # host
    if info.get("overview_levels"):
        # gridlook shows only the root; the maps viewer lists entries with this tag.
        gridlook["tags"]["overviews"] = f"{OVERVIEW_GROUP} ({info['overview_levels']} levels)"

    storage_options: dict = {}
    if target.xreds_endpoint:
        storage_options |= {"endpoint_url": target.xreds_endpoint,
                            "allow_http": target.xreds_endpoint.startswith("http://"),
                            "force_path_style": True}
    if info["prefix"]:
        # Needs the patched Xreds (xreds/virtual-icechunk-s3-http.patch).
        storage_options["virtual_chunk_container"] = {"type": "http", "store": {"path": info["prefix"]}}
    xreds = {"path": target.xreds_base + name, "type": "virtual-icechunk", "chunks": {},
             "storage_options": storage_options}
    # Gridlook-only alias coordinates are hidden from Xreds; ROMS output gets
    # Xreds' roms extension (rotated currents) and, like upstream's ROMS
    # examples, drops dstart (a run-start scalar, not data).
    drop = coord_aliases_in(info)
    # Best-time-series provenance: cf_xarray's guess_coord_axis() would take
    # forecast_reference_time for a second T axis; WMS/EDR has no use for either.
    drop += [c for c in PROVENANCE if c in info["variables"]]
    roms = is_roms(info)
    if roms and "dstart" in info["variables"]:
        drop.append("dstart")
    if drop:
        xreds["drop_variables"] = drop
    if roms:
        xreds["extensions"] = {"roms": {}}
    return gridlook, (dataset_id, xreds)


def write_catalogs(target: CatalogTarget, gridlook: dict, xreds: dict) -> None:
    write_root_file(target.root, GRIDLOOK_CATALOG, json.dumps(gridlook, indent=2) + "\n", "application/json")
    header = "# Generated by virtualize.py; entries are keyed by repository name.\n"
    write_root_file(target.root, XREDS_CATALOG, header + yaml.safe_dump(xreds, sort_keys=False), "application/yaml")
    where = target.root if target.root.startswith("s3://") else os.path.relpath(target.root)
    log.info("catalogs: %s/%s; gridlook: %s%s", where, CATALOG_DIR,
             target.public_base, GRIDLOOK_CATALOG)


def new_gridlook_catalog() -> dict:
    return {"type": "gridlook_catalog", "title": "Virtual Icechunk stores", "datasets": []}


def upsert_catalogs(target: CatalogTarget, name: str, storage: ic.Storage) -> None:
    """Create or update only `name`'s entry in both catalogs; other entries are
    kept as they are, in their order."""
    info = describe_repo(storage)
    if info is None:
        log.warning("catalogs: %s holds no data; not listed", name)
        return
    entry, (dataset_id, xentry) = catalog_entries(target, name, info)

    text = read_root_file(target.root, GRIDLOOK_CATALOG)
    gridlook = json.loads(text) if text else new_gridlook_catalog()
    datasets = gridlook.setdefault("datasets", [])
    same = [i for i, d in enumerate(datasets) if d.get("url") == entry["url"]]
    if same:
        datasets[same[0]] = entry
        for i in reversed(same[1:]):
            del datasets[i]
    else:
        datasets.append(entry)

    text = read_root_file(target.root, XREDS_CATALOG)
    xreds = (yaml.safe_load(text) if text else None) or {}
    xreds[dataset_id] = xentry
    write_catalogs(target, gridlook, xreds)


def scan_catalogs(target: CatalogTarget) -> None:
    """Rewrite both catalogs from the repositories found under the root."""
    gridlook, xreds = new_gridlook_catalog(), {}
    old = read_root_file(target.root, GRIDLOOK_CATALOG)
    if old:
        gridlook["title"] = json.loads(old).get("title", gridlook["title"])
    for name in list_repos(target.root):
        repo_arg = f"{target.root}/{name}"
        info = describe_repo(make_storage(repo_arg))
        if info is None:
            log.warning("catalogs: %s holds no data; skipped", name)
            continue
        entry, (dataset_id, xentry) = catalog_entries(target, name, info)
        gridlook["datasets"].append(entry)
        xreds[dataset_id] = xentry
        log.info("catalogs: %s: %s", name, entry["description"])
    log.info("catalogs: %d repositor%s found", len(xreds), "y" if len(xreds) == 1 else "ies")
    write_catalogs(target, gridlook, xreds)


def main() -> None:
    # Destination settings (AWS_*) from .env next to the script; the shell's
    # environment wins over the file.
    load_dotenv(Path(__file__).with_name(".env"), override=False)
    # --config decides which presets exist, so read it before building the full
    # parser (whose dataset choices come from that file).
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", type=Path, default=DATASETS_FILE,
                     help=f"dataset presets file (default: {DATASETS_FILE.name} next to this script)")
    cat = pre.add_argument_group(
        "catalogs",
        f"After a build, the repository's entry in {GRIDLOOK_CATALOG} and {XREDS_CATALOG} (in "
        f"{CATALOG_DIR}/ next to the repository) is created or updated; other entries are left alone.")
    cat.add_argument("--scan-catalogs", metavar="ROOT",
                     help="only rebuild both catalogs from all *.icechunk repositories under ROOT "
                          "(local directory or s3://bucket[/prefix]) and exit; no dataset or dates needed")
    cat.add_argument("--public-base-url", metavar="URL",
                     help="browser URL of the repository root, for gridlook (default: from AWS_ENDPOINT_URL "
                          f"for s3:// roots, {LOCAL_PUBLIC_BASE} for local ones)")
    cat.add_argument("--xreds-base-url", metavar="URL",
                     help=f"s3:// URL of the repository root as Xreds sees it (default: the s3:// root itself, "
                          f"{LOCAL_XREDS_BASE} for local roots)")
    cat.add_argument("--xreds-endpoint-url", metavar="URL",
                     help=f"S3 endpoint Xreds uses (default: {COMPOSE_S3_ENDPOINT} when AWS_ENDPOINT_URL "
                          "is set or the root is local, else AWS; '' for AWS)")
    known, extra = pre.parse_known_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if known.scan_catalogs:
        if extra:
            pre.error(f"--scan-catalogs takes only the catalog options, not: {' '.join(extra)}")
        scan_catalogs(catalog_target(known.scan_catalogs, known.public_base_url,
                                     known.xreds_base_url, known.xreds_endpoint_url))
        return
    config_path = known.config
    if not config_path.is_file():
        pre.error(f"--config: no such file: {config_path}")
    datasets = load_datasets(config_path)

    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], parents=[pre],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dataset", choices=sorted(datasets), help=f"dataset preset from {config_path.name}")
    p.add_argument("--repo", help="repository: local directory or s3://bucket/prefix")
    p.add_argument("--unsafe-no-conditional-writes", action="store_true",
                   help="disable conditional writes (automatic for Backblaze B2 S3 endpoints); "
                        "saved in the repository; requires only one writer at a time, "
                        "including builds and garbage collection")
    p.add_argument("--start", type=parse_time, required=True,
                   help="first valid time, inclusive: YYYY-MM-DD or YYYY-MM-DDTHH[:MM[:SS]], UTC; "
                        "or today / today-N (whole days)")
    p.add_argument("--end", type=parse_time,
                   help="last valid time, inclusive (UTC); a date alone means the whole day, "
                        "up to 00:00 of the next. Optional for presets with `sources`: then "
                        "everything published so far (e.g. the whole forecast)")
    p.add_argument("--batch-size", type=int, default=32, help="files per commit (default 32)")
    p.add_argument("--workers", type=int, default=8, help="parallel file-opening processes (default 8)")
    p.add_argument("--dry-run", action="store_true", help="list the files and exit")
    p.add_argument("--no-validate", action="store_true", help="skip the final read-back check")
    p.add_argument("--force", action="store_true",
                   help="re-ingest files whose time steps are already in the store, "
                        "replacing their references in place (instead of skipping them)")
    # Overrides for any Dataset field, so a new grid dataset needs no code change.
    p.add_argument("--bucket-url"); p.add_argument("--http-base"); p.add_argument("--key-root")
    p.add_argument("--key-template"); p.add_argument("--file-regex"); p.add_argument("--concat-dim")
    p.add_argument("--loadable", help="comma-separated variables to materialize")
    p.add_argument("--time-chunk", type=int)
    p.add_argument("--folder-margin-days", type=int,
                   help="day folders scanned beyond each end of the range (default 1)")
    p.add_argument("--drop-global-attrs",
                   help="comma-separated global attribute names to drop (wildcards allowed, e.g. 'frc_file_*')")
    p.add_argument("--coord-aliases",
                   help="comma-separated alias=source coordinate aliases, e.g. 'lat=lat_rho,lon=lon_rho' ('' for none)")
    p.add_argument("--rectilinear",
                   help="regular grid given as 2-D arrays, 'lat=NAME,lon=NAME' ('' for none)")
    p.add_argument("--no-catalog", action="store_true", help="do not update the catalogs after the build")
    p.add_argument("--retain", type=parse_duration, default=parse_duration("3d"), metavar="AGE",
                   help="after a build that committed, expire snapshots older than AGE (e.g. 3d, 12h, "
                        "0 = keep only the latest) and garbage-collect what only they used (default 3d)")
    p.add_argument("--no-gc", dest="retain", action="store_const", const=None,
                   help="keep all snapshots and data (no expiry or garbage collection)")
    srcg = p.add_argument_group(
        "source reads",
        "How overview builds and validation read the source (defaults, or the preset's source_* fields). "
        "On a network where connects time out, lower --source-concurrency.")
    srcg.add_argument("--source-concurrency", type=int, metavar="N",
                      help="concurrent HTTP requests (default 32)")
    srcg.add_argument("--source-connect-timeout", type=int, metavar="SECONDS",
                      help="TCP/TLS connect timeout (default 30)")
    srcg.add_argument("--source-timeout", type=int, metavar="SECONDS", help="timeout per request (default 300)")
    srcg.add_argument("--source-http-retries", type=int, metavar="N",
                      help="tries per chunk request, including the first (default 10)")
    srcg.add_argument("--source-read-retries", type=int, metavar="N",
                      help="retries of a whole failed read, 30 s, 60 s, ... apart (default 5)")
    ovg = p.add_argument_group(
        "multiscale overviews",
        "Coarsened copies of selected variables in the group 'multiscale' (GeoZarr multiscales), "
        "for web maps. Needs a regular lat/lon grid. Fixed when a repository is created; later "
        "builds keep them up to date.")
    ovg.add_argument("--overviews", action=argparse.BooleanOptionalAction, default=None,
                     help="build overviews in a new repository (default: if the preset has an "
                          "`overviews` entry)")
    ovg.add_argument("--overview-variables", metavar="A,B", help="variables to build overviews for")
    ovg.add_argument("--overview-levels", metavar="N|auto",
                     help="number of levels including the full-resolution level 0 (default 3)")
    ovg.add_argument("--overview-method", choices=OVERVIEW_METHODS, help="reduction (default mean)")
    ovg.add_argument("--overview-keep-mantissa-bits", type=int, choices=range(24), metavar="0..23",
                     help="lossy float32 overview rounding (default disabled; 23 is a no-op)")
    ovg.add_argument("--overview-keep-mantissa-bits-by-variable", action="append",
                     type=parse_variable_rounding_arg, metavar="VARIABLE=BITS|none",
                     help="override a variable's overview rounding; repeat for multiple variables")
    args = p.parse_args()

    lo, hi, incl = time_bounds(args.start, args.end)
    if hi is not None and (hi < lo or (hi == lo and not incl)):
        p.error("--end is before --start")
    if not args.dry_run and not args.repo:
        p.error("--repo is required unless --dry-run")

    overrides = {k: v for k, v in ({
        "bucket_url": args.bucket_url, "http_base": args.http_base, "key_root": args.key_root,
        "key_template": args.key_template, "file_regex": args.file_regex,
        "concat_dim": args.concat_dim, "time_chunk": args.time_chunk,
        "folder_margin_days": args.folder_margin_days,
        "drop_global_attrs": tuple(args.drop_global_attrs.split(",")) if args.drop_global_attrs else None,
        "loadable": tuple(args.loadable.split(",")) if args.loadable else None,
        "coord_aliases": None if args.coord_aliases is None else
            dict(pair.split("=", 1) for pair in args.coord_aliases.split(",") if pair),
        "rectilinear": None if args.rectilinear is None else
            dict(pair.split("=", 1) for pair in args.rectilinear.split(",") if pair),
    } | {k: getattr(args, k) for k in SOURCE_FIELDS}).items() if v is not None}
    if args.file_regex and datasets[args.dataset].sources:
        p.error(f"{args.dataset} is defined by `sources`; --file-regex does not apply")
    ds = dataclasses.replace(datasets[args.dataset], **overrides)
    if args.end is None and not ds.sources:
        p.error("--end is required (it is optional only for presets with `sources`)")
    for k in SOURCE_FIELDS:
        if getattr(ds, k) < (0 if k == "source_read_retries" else 1):
            p.error(f"--{k.replace('_', '-')} must be positive")
    if ds.folder_margin_days < 0:
        p.error("--folder-margin-days must be >= 0")
    if ds.concat_dim not in ds.loadable:
        p.error("--loadable must include the concat dimension")
    if ds.rectilinear:
        if set(ds.rectilinear) != {"lat", "lon"}:
            p.error("--rectilinear needs exactly lat=NAME,lon=NAME")
        if not set(ds.rectilinear.values()) <= set(ds.loadable):
            p.error("the rectilinear lat/lon arrays must be in --loadable")
    if args.overview_levels:
        parse_levels(args.overview_levels)
    if args.overviews and not ds.rectilinear:
        p.error("--overviews needs a regular lat/lon grid (`rectilinear`); this dataset's grid is "
                "curvilinear or undeclared. Use a *-regulargrid-* preset")
    if not (ds.http_base.endswith("/") and ds.key_root.endswith("/")):
        p.error("http_base and key_root must end in '/'")
    log.info("config: %s", json.dumps(dataclasses.asdict(ds)))
    build(args, ds, args.dataset)


if __name__ == "__main__":
    main()
