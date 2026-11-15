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

    # See what would be indexed, without touching the repository
    ./virtualize.py wcofs-fields-nowcast --start 2026-10-01 --end 2026-10-03 --dry-run

    # Same, but into the local versitygw S3 container (docker compose up -d)
    ./virtualize.py wcofs-fields-nowcast --repo s3://icechunk/wcofs.icechunk \\
        --start 2026-10-01 --end 2026-10-31

    # Regular-grid files, with multiscale overviews for web maps (preset default)
    ./virtualize.py wcofs-regulargrid-nowcast --repo s3://icechunk/wcofs-rg.icechunk \\
        --start 2026-11-01 --end 2026-11-07

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
class Dataset:
    # Source: anonymous public object storage. Listing uses the bucket URL;
    # references embed the public HTTPS URL, which any reader can use.
    bucket_url: str  # e.g. "s3://noaa-nos-ofs-pds" (listing)
    http_base: str  # e.g. "https://noaa-nos-ofs-pds.s3.amazonaws.com/" (references)
    key_root: str  # prefix inside the bucket that holds the dataset, ends in "/"
    key_template: str  # per-day folder under key_root; strftime fields from the date
    file_regex: str  # which files in a day folder belong to this stream
    concat_dim: str  # dimension the files are appended along
    # Variables to materialize (small ones only!). The concat coordinate must be
    # included. Everything else stays a virtual reference.
    loadable: tuple[str, ...] = ()
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
    # {variables: [...], levels: 3 | auto, method: mean|min|max|nearest}.
    # Present = built by default; --overviews/--no-overviews override.
    # Needs `rectilinear` (or 1-D lat/lon coordinates).
    overviews: dict | None = None

    @property
    def url_prefix(self) -> str:
        return self.http_base + self.key_root


DATASETS_FILE = Path(__file__).with_name("datasets.yml")


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
        if "overviews" in fields:
            ov = fields["overviews"]
            if ov is None:
                ov = {}
            if not isinstance(ov, dict) or set(ov) - {"variables", "levels", "method"}:
                raise SystemExit(f"{path}: dataset {name!r}: overviews must be a mapping with "
                                 "variables, levels and/or method")
            if "variables" in ov and not isinstance(ov["variables"], list):
                raise SystemExit(f"{path}: dataset {name!r}: overviews.variables must be a list")
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


def discover(ds: Dataset, start: dt.date, end: dt.date) -> list[str]:
    """Return the sorted public HTTPS URLs of all files for days start..end."""
    store = source_store(ds, bucket=True)
    pattern = re.compile(ds.file_regex)
    urls: list[str] = []
    empty_days: list[str] = []
    day = start
    while day <= end:
        prefix = ds.key_root + day.strftime(ds.key_template)
        found = [
            o["path"]
            for batch in store.list(prefix=prefix)
            for o in batch
            if pattern.match(o["path"].rsplit("/", 1)[-1])
        ]
        if not found:
            empty_days.append(day.isoformat())
        urls += [ds.http_base + p for p in found]
        day += dt.timedelta(days=1)
    if empty_days:
        # A build that quietly drops days is worse than one that says so.
        log.warning("no matching files for %d day(s): %s", len(empty_days), ", ".join(empty_days))
    return sorted(urls)


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


def s3_options() -> dict:
    """ic.s3_storage keyword arguments from the standard AWS variables.

    AWS_ENDPOINT_URL selects an S3-compatible server (e.g. the local versitygw
    container); without it this is plain AWS. Explicit keys win; otherwise the
    AWS SDK's own lookup (profiles, SSO, instance metadata) is used."""
    env = lambda k: os.environ.get(k) or None  # noqa: E731 (empty counts as unset)
    endpoint = env("AWS_ENDPOINT_URL")
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


def open_or_create_repo(storage: ic.Storage, ds: Dataset) -> tuple[ic.Repository, bool]:
    """Create the repository if (and only if) it does not exist; any other error
    (auth, network, config) must surface, so there is no blanket except."""
    auth = {ds.url_prefix: ic.credentials.HttpAccess}  # anonymous HTTP, but still explicit
    container = ic.VirtualChunkContainer(url_prefix=ds.url_prefix, store=ic.http_store())
    if not ic.Repository.exists(storage):
        config = ic.RepositoryConfig.default()
        config.set_virtual_chunk_container(container)
        repo = ic.Repository.create(storage, config=config, authorize_virtual_chunk_access=auth)
        repo.save_config()  # persist, so public readers discover the container
        return repo, True
    repo = ic.Repository.open(storage, authorize_virtual_chunk_access=auth)
    known = (repo.config.virtual_chunk_containers or {}).keys()
    if ds.url_prefix not in known:
        repo.config.set_virtual_chunk_container(container)
        repo.save_config()
        repo = ic.Repository.open(storage, authorize_virtual_chunk_access=auth)
    return repo, False


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


@dataclass(frozen=True)
class Overviews:
    variables: tuple[str, ...]  # empty: every variable over (time, ..., lat, lon)
    levels: int | str = 3  # levels including level 0, or "auto" (topozarr's min_dim rule)
    method: str = "mean"


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
    return Overviews(variables, len(layout), method)


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
            if args.overview_variables or args.overview_levels or args.overview_method:
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
    method = str(cfg.get("method", "mean"))
    if method not in OVERVIEW_METHODS:
        raise SystemExit(f"overviews: method must be one of {', '.join(OVERVIEW_METHODS)}")
    if not ds.rectilinear:
        raise SystemExit("overviews need a regular lat/lon grid (see `rectilinear`); this "
                         "dataset's grid is not declared regular. Curvilinear grids (e.g. the ROMS "
                         "`fields` files) cannot have overviews: use the `regulargrid` presets")
    return Overviews(tuple(str(v) for v in cfg.get("variables") or ()),
                     parse_levels(cfg.get("levels", 3)), method)


def overview_variables(vds: xr.Dataset, ov: Overviews, dim: str) -> list[str]:
    spatial = {dim, "lat", "lon"}
    eligible = [n for n, v in vds.data_vars.items() if spatial <= set(v.dims)]
    if not ov.variables:
        return eligible
    bad = [v for v in ov.variables if v not in eligible]
    if bad:
        raise SystemExit(f"overviews: {', '.join(bad)} not in the files or not over "
                         f"({dim}, lat, lon); candidates: {', '.join(eligible)}")
    return list(ov.variables)


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


def update_overviews(session: ic.Session, vds: xr.Dataset, ov: Overviews, dim: str,
                     start: int, *, create: bool = False, replace: bool = False,
                     time_chunk: int = 4096) -> None:
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
        p1 = min(start + n, p0 + per_pass)
        data: list[dict[str, np.ndarray]] = [{} for _ in range(levels)]
        for v in variables:
            a = lvl0[v].isel({dim: slice(p0, p1)}).values  # the only source read
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
                {v: (lvl0[v].dims, data[k][v], dict(lvl0[v].attrs)) for v in variables},
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
        log.info("overviews: %s steps %d..%d of %s -> levels 1..%d",
                 "rewrote" if replace else "wrote", p0, p1 - 1, ",".join(variables), levels - 1)
    log.info("overviews: %d step(s) in %.0fs", n, time.time() - t_start)


def validate_overviews(store, dim: str, root_times: np.ndarray) -> None:
    """Every level holds the root's time steps at its declared shape, and the last
    step of each level is the reduction of the level below it."""
    root = zarr.open_group(store, path=OVERVIEW_GROUP, mode="r")
    layout = root.attrs["multiscales"]["layout"]
    method = next((e.get("resampling_method") for e in layout[1:] if e.get("resampling_method")), "mean")
    levels = [xr.open_zarr(store, group=f"{OVERVIEW_GROUP}/{e['asset']}", consolidated=False, chunks=None)
              for e in layout]
    for k, (entry, lv) in enumerate(zip(layout, levels)):
        assert (lv[dim].values == root_times).all(), f"overview level {k}: time steps differ from the root"
        assert (lv.sizes["lat"], lv.sizes["lon"]) == tuple(entry["spatial:shape"]), \
            f"overview level {k}: shape differs from its metadata"
    # Smallest variable per step keeps the read-back cheap.
    var = min(levels[1].data_vars, key=lambda v: math.prod(levels[1][v].shape[1:]))
    sel = {d: 0 for d in levels[0][var].dims if d not in (dim, "lat", "lon")} | {dim: -1}
    prev = levels[0][var].isel(sel).values
    for k in range(1, len(levels)):
        got = levels[k][var].isel(sel).values
        assert np.allclose(reduce2(prev, method), got, equal_nan=True, rtol=1e-5, atol=1e-6), \
            f"overview level {k} of {var} is not the {method} of level {k - 1}"
        prev = got
    log.info("validated overviews: %d levels (%s), %s at the last step matches level by level",
             len(levels), " / ".join(f"{lv.sizes['lat']}x{lv.sizes['lon']}" for lv in levels), var)


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def build(args: argparse.Namespace, ds: Dataset, name: str) -> None:
    dim = ds.concat_dim
    zarr.config.set({"async.concurrency": 128})

    urls = discover(ds, args.start, args.end)
    log.info("discovered %d files for %s..%s", len(urls), args.start, args.end)
    if not urls:
        raise SystemExit("nothing to index")
    if args.dry_run:
        for u in urls:
            print(u)
        return

    storage = make_storage(args.repo)
    repo, created = open_or_create_repo(storage, ds)
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

    if not args.force:
        already = indexed_files(repo, ds)
        pending = [u for u in urls if u not in already]
        if len(pending) < len(urls):
            log.info("skipping %d file(s) recorded as already indexed", len(urls) - len(pending))
        urls = pending

    total_new = total_replaced = 0
    for i in range(0, len(urls), args.batch_size):
        batch_urls = urls[i : i + args.batch_size]
        vds_list = open_files(ds, batch_urls, args.workers)
        times = np.array([v[dim].values[0] for v in vds_list], dtype="datetime64[ns]")

        # Reconcile against committed state: files whose valid time is already in
        # the store were indexed by an earlier run. Skip them, unless --force,
        # in which case their references are rewritten in place below.
        keep = ~np.isin(times, have) | args.force
        if not keep.all():
            log.info("batch %d: skipping %d already-indexed file(s)", i // args.batch_size, int((~keep).sum()))
        vds_list = [v for v, k in zip(vds_list, keep) if k]
        batch_urls = [u for u, k in zip(batch_urls, keep) if k]
        times = times[keep]
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
                    update_overviews(session, part, ov, dim, int(idx[run[0]]), replace=True)
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
                vds.vz.to_icechunk(session.store)
                if ov:
                    update_overviews(session, vds, ov, dim, 0, create=True, time_chunk=ds.time_chunk)
            else:
                static = [n for n, v in vds.variables.items() if dim not in v.dims]
                appended = vds.drop_vars(static)
                appended.attrs = {}  # appending would otherwise overwrite the global attrs
                appended.vz.to_icechunk(session.store, append_dim=dim)
                if ov:
                    update_overviews(session, vds, ov, dim, int(have.size), time_chunk=ds.time_chunk)
        n_new, n_replaced = int(new.sum()), int(replace.sum())
        verb = "index" if not n_replaced else "re-index" if not n_new else "index/re-index"
        snapshot = session.commit(
            f"{verb} {len(vds_list)} files: {batch_urls[0].rsplit('/', 1)[-1]} .. {batch_urls[-1].rsplit('/', 1)[-1]}",
            metadata={
                "dataset": name,
                "files": [u.removeprefix(ds.url_prefix) for u in batch_urls],
                "replaced": [u.removeprefix(ds.url_prefix) for u, r in zip(batch_urls, replace) if r],
                "first_time": str(times[0]),
                "last_time": str(times[-1]),
                "overviews": ov.levels if ov else None,
                "versions": versions(),
            },
        )  # a committed session is read-only; the next batch opens a fresh one
        have = np.concatenate([have, times[new]])
        total_new += n_new
        total_replaced += n_replaced
        log.info("committed %s: +%d new, %d replaced step(s) (%s .. %s)",
                 snapshot, n_new, n_replaced, times[0], times[-1])

    log.info("done: %d new, %d replaced step(s), %d total", total_new, total_replaced, have.size)
    if not args.no_validate:
        validate(storage, ds, expected_steps=have.size, expected_last=have[-1] if have.size else None)
    if not args.no_catalog:
        root, repo_name = split_repo(args.repo)
        target = catalog_target(root, args.public_base_url, args.xreds_base_url, args.xreds_endpoint_url)
        upsert_catalogs(target, repo_name, storage)


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
    sample = da.isel(window).isel({ds.concat_dim: -1}).load()  # fetches a source chunk
    log.info(
        "validated: %d steps, %s .. %s; read %s%s from the source (dtype %s, %d finite of %d)",
        t.size, t[0], t[-1], var, tuple(sample.shape), sample.dtype,
        int(np.isfinite(sample.values).sum()), sample.size,
    )
    if stored_overviews(repo, ds.concat_dim) is not None:
        validate_overviews(store, ds.concat_dim, t)


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
    for name, arr in arrays.items():
        a = arr.attrs
        if arr.ndim == 1 and (a.get("axis") == "T" or a.get("standard_name") == "time"):
            # Decode just this (materialized) array; opening the whole dataset
            # would read virtual scalars, which needs chunk authorization.
            var = xr.Variable(("t",), arr[:], {k: v for k, v in a.items() if k != "_ARRAY_DIMENSIONS"})
            t = xr.decode_cf(xr.Dataset({"t": var}))["t"].values
            if t.size:
                steps, t0, t1 = t.size, str(t[0])[:16], str(t[-1])[:16]
            break
    try:
        layout = group[OVERVIEW_GROUP].attrs.get("multiscales", {}).get("layout", [])
    except KeyError:
        layout = []
    return {"attrs": attrs, "prefix": attrs.get("virtualized_source") or (prefixes[0] if prefixes else None),
            "steps": steps, "first": t0, "last": t1, "variables": variables, "dims": dims,
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


def parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


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
    p.add_argument("--start", type=parse_date, required=True, help="first day (YYYY-MM-DD), by folder date")
    p.add_argument("--end", type=parse_date, required=True, help="last day, inclusive")
    p.add_argument("--batch-size", type=int, default=64, help="files per commit (default 64)")
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
    p.add_argument("--drop-global-attrs",
                   help="comma-separated global attribute names to drop (wildcards allowed, e.g. 'frc_file_*')")
    p.add_argument("--coord-aliases",
                   help="comma-separated alias=source coordinate aliases, e.g. 'lat=lat_rho,lon=lon_rho' ('' for none)")
    p.add_argument("--rectilinear",
                   help="regular grid given as 2-D arrays, 'lat=NAME,lon=NAME' ('' for none)")
    p.add_argument("--no-catalog", action="store_true", help="do not update the catalogs after the build")
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
    args = p.parse_args()

    if args.end < args.start:
        p.error("--end is before --start")
    if not args.dry_run and not args.repo:
        p.error("--repo is required unless --dry-run")

    overrides = {k: v for k, v in {
        "bucket_url": args.bucket_url, "http_base": args.http_base, "key_root": args.key_root,
        "key_template": args.key_template, "file_regex": args.file_regex,
        "concat_dim": args.concat_dim, "time_chunk": args.time_chunk,
        "drop_global_attrs": tuple(args.drop_global_attrs.split(",")) if args.drop_global_attrs else None,
        "loadable": tuple(args.loadable.split(",")) if args.loadable else None,
        "coord_aliases": None if args.coord_aliases is None else
            dict(pair.split("=", 1) for pair in args.coord_aliases.split(",") if pair),
        "rectilinear": None if args.rectilinear is None else
            dict(pair.split("=", 1) for pair in args.rectilinear.split(",") if pair),
    }.items() if v is not None}
    ds = dataclasses.replace(datasets[args.dataset], **overrides)
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
