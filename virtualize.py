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
import os
import re
import sys
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
        if "coord_aliases" in fields:
            if not isinstance(fields["coord_aliases"], dict):
                raise SystemExit(f"{path}: dataset {name!r}: coord_aliases must be a mapping")
            fields["coord_aliases"] = {str(k): str(v) for k, v in fields["coord_aliases"].items()}
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
    with ProcessPoolExecutor(max_workers=workers) as pool:
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
            else:
                static = [n for n, v in vds.variables.items() if dim not in v.dims]
                appended = vds.drop_vars(static)
                appended.attrs = {}  # appending would otherwise overwrite the global attrs
                appended.vz.to_icechunk(session.store, append_dim=dim)
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
    config_path = pre.parse_known_args()[0].config
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
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
    }.items() if v is not None}
    ds = dataclasses.replace(datasets[args.dataset], **overrides)
    if ds.concat_dim not in ds.loadable:
        p.error("--loadable must include the concat dimension")
    if not (ds.http_base.endswith("/") and ds.key_root.endswith("/")):
        p.error("http_base and key_root must end in '/'")
    log.info("config: %s", json.dumps(dataclasses.asdict(ds)))
    build(args, ds, args.dataset)


if __name__ == "__main__":
    main()
