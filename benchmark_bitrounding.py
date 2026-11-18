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
"""Compare actual overview writes in disposable local Icechunk repositories.

Use --source-url for a single source file. Decoded arrays are fetched once and
replayed from temporary .npy files on subsequent writes, keeping level 0 virtual.
No catalogs, production repositories, or source files are modified.
"""
import argparse
import dataclasses
import datetime as dt
import importlib.metadata
import json
import logging
from pathlib import Path
import platform
import resource
import sys
import tempfile
import time
from unittest.mock import patch

import icechunk as ic
import numpy as np
import xarray as xr
import zarr
from zarr.core.sync import sync

import virtualize as v


class SourceCache:
    def __init__(self, directory, retries):
        self.directory = Path(directory)
        self.retries = retries
        self.files = {}
        self.seconds = 0.0
        self.reader = v.read_source

    def __call__(self, fn, what, retries):
        if what not in self.files:
            started = time.perf_counter()
            array = self.reader(fn, what, self.retries)
            self.seconds += time.perf_counter() - started
            path = self.directory / f"{len(self.files)}.npy"
            np.save(path, array)
            self.files[what] = path
        return np.load(self.files[what], mmap_mode="r")


def error_metrics(expected, got):
    if not np.array_equal(np.isnan(expected), np.isnan(got)):
        raise AssertionError("rounding changed the missing-value mask")
    finite = np.isfinite(expected)
    reference = expected[finite].astype("f8")
    observed = got[finite].astype("f8")
    if not np.isfinite(observed).all():
        raise AssertionError("finite source values overflowed during rounding")
    error = np.abs(observed - reference)
    # Absolute error and RMSE include zero. Relative error excludes a documented
    # near-zero interval, chosen independently for each variable and level.
    threshold = max(float(np.max(np.abs(reference), initial=0)) * 1e-6, 1e-12)
    relative = np.abs(reference) > threshold
    return {"finite_values": int(reference.size),
            "max_absolute_error": float(np.max(error, initial=0)),
            "rmse": float(np.sqrt(np.mean(error ** 2))) if error.size else 0.0,
            "relative_error_min_abs": threshold,
            "max_relative_error": float(np.max(error[relative] / np.abs(reference[relative]), initial=0))}


def benchmark(args):
    ds = v.load_datasets()[args.dataset]
    if not ds.rectilinear or not ds.overviews:
        raise SystemExit("benchmark needs a regular-grid preset with overviews")
    if not args.source_url.startswith(ds.url_prefix):
        raise SystemExit(f"--source-url must start with {ds.url_prefix}")
    zarr.config.set({"async.concurrency": ds.source_concurrency})
    raw = v.open_files(ds, [args.source_url], workers=1)
    source = v.combine(raw, ds).isel({ds.concat_dim: slice(0, 1)})
    base = v.Overviews(tuple(ds.overviews.get("variables", ())),
                       v.parse_levels(ds.overviews.get("levels", 3)),
                       ds.overviews.get("method", "mean"))
    variables = v.overview_variables(source, base, ds.concat_dim)
    report = {"dataset": args.dataset, "source_url": args.source_url,
              "measured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
              "platform": platform.platform(), "source_concurrency": ds.source_concurrency,
              "valid_time": str(source[ds.concat_dim].values[0]),
              "versions": v.versions() | {p: importlib.metadata.version(p)
                                          for p in ["numpy", "topozarr"]},
              "notes": ["Source arrays fetched once, then replayed from local memory-mapped files.",
                        "Elapsed write time includes rounding, encoding and commit; excludes source fetches.",
                        "Peak RSS is the process high-water mark, including verification and earlier variants.",
                        "Chunk bytes include shard indexes; repository bytes also include metadata and coordinates.",
                        "Relative errors exclude abs(value) <= max(max_abs_value * 1e-6, 1e-12)."],
              "variants": []}
    with tempfile.TemporaryDirectory(prefix="overview-bitround-") as scratch:
        cache_dir = Path(scratch) / "source-cache"
        cache_dir.mkdir()
        cache = SourceCache(cache_dir, ds.source_read_retries)
        # Fetch before timing any variant, so the baseline does not pay for
        # writing the cache while later variants only pay for replaying it.
        cache_repo, _ = v.open_or_create_repo(ic.local_filesystem_storage(str(Path(scratch) / "cache-refs")), ds)
        cache_session = cache_repo.writable_session("main")
        source[variables].vz.to_icechunk(cache_session.store)
        decoded = xr.open_zarr(cache_session.store, consolidated=False, chunks=None)
        for var in variables:
            logging.info("benchmark: fetching %s once", var)
            cache(lambda var=var: decoded[var].values, f"{var} steps 0..0", 0)
        baseline_bytes = None
        for keep in [None, 10, 12, 14, 16]:
            label = "disabled" if keep is None else str(keep)
            logging.info("benchmark: %s retained bits %s", args.dataset, label)
            path = Path(scratch) / label
            storage = ic.local_filesystem_storage(str(path))
            repo, _ = v.open_or_create_repo(storage, ds)
            session = repo.writable_session("main")
            # Include the regular root so physical size describes the real hybrid
            # layout, with both root and level 0 pointing at the source file.
            source.vz.to_icechunk(session.store)
            before_fetch = cache.seconds
            started = time.perf_counter()
            with patch.object(v, "read_source", cache):
                v.update_overviews(session, source, dataclasses.replace(base, keep_mantissa_bits=keep),
                                   ds.concat_dim, 0, create=True, time_chunk=ds.time_chunk)
            session.commit(f"bitround benchmark {label}")
            elapsed = time.perf_counter() - started - (cache.seconds - before_fetch)
            # Reopen through the consumer path; verify every variable/depth/level.
            repo = ic.Repository.open(storage, authorize_virtual_chunk_access={
                ds.url_prefix: ic.credentials.HttpAccess})
            session = repo.readonly_session("main")
            store = session.store
            ov = v.stored_overviews(repo, ds.concat_dim)
            levels = ov.levels
            rows = []
            for var in variables:
                expected = cache(lambda: None, f"{var} steps 0..0", 0)
                for level in range(1, levels):
                    expected = v.reduce2(expected, base.method)
                    got = xr.open_zarr(store, group=f"multiscale/{level}",
                                       consolidated=False, chunks=None)[var].values
                    rounded = v.round_float32(expected, keep) if keep is not None else expected
                    np.testing.assert_array_equal(got, rounded)
                    array_path = f"multiscale/{level}/{var}"
                    chunk_bytes = (sync(store.getsize_prefix(array_path))
                                   - sync(store.getsize(array_path + "/zarr.json")))
                    array = zarr.open_array(store, path=array_path, mode="r")
                    rows.append({"variable": var, "level": level, "shape": list(got.shape),
                                 "chunk_bytes": chunk_bytes, "codecs": array.metadata.to_dict()["codecs"],
                                 **error_metrics(expected, got)})
            chunk_bytes = sum(row["chunk_bytes"] for row in rows)
            if baseline_bytes is None:
                baseline_bytes = chunk_bytes
            report["variants"].append({"keep_mantissa_bits": keep, "overview_chunk_bytes": chunk_bytes,
                "savings_fraction": 1 - chunk_bytes / baseline_bytes,
                "repository_bytes": sum(p.stat().st_size for p in path.rglob("*") if p.is_file()),
                "write_seconds_excluding_source_fetch": elapsed,
                "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    * (1 if sys.platform == "darwin" else 1024),
                "virtual_source_locations": sorted(set(session.all_virtual_chunk_locations())), "arrays": rows})
            logging.info("benchmark: %s: %d overview bytes (%.1f%% smaller), %.2fs", label,
                         chunk_bytes, 100 * (1 - chunk_bytes / baseline_bytes), elapsed)
        report["source_fetch_seconds"] = cache.seconds
        report["source_arrays_fetched"] = len(cache.files)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=v.load_datasets())
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--output", type=Path, required=True, help="JSON measurement report")
    options = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    options.output.write_text(json.dumps(benchmark(options), indent=2, allow_nan=False) + "\n")
