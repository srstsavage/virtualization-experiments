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
"""Run with uv run --script test_insertion.py. All repositories are temporary."""
import argparse
import asyncio
import dataclasses
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import threading
import unittest
import warnings
from unittest.mock import patch

import icechunk as ic
import numpy as np
import xarray as xr
import zarr
from virtualizarr.manifests import ChunkManifest, ManifestArray

import virtualize as v
from test_bitrounding import sample


class InsertionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = ic.local_filesystem_storage(self.tmp.name + "/repo")
        self.config = v.Dataset("s3://test", "https://example.invalid/", "data/", "%Y/", "time",
                                file_regex=".*", loadable=("time",))
        self.args = argparse.Namespace(
            repo=self.tmp.name + "/repo", start=v.parse_time("2026-11-01"),
            end=v.parse_time("2026-11-02"), dry_run=False, force=False, batch_size=2,
            workers=1, unsafe_no_conditional_writes=False, retain=None,
            no_validate=True, no_catalog=True, overviews=None, overview_variables=None,
            overview_levels=None, overview_method=None, overview_keep_mantissa_bits=None,
            overview_keep_mantissa_bits_by_variable=None)
        self.warning_context = warnings.catch_warnings()
        self.warning_context.__enter__()
        self.addCleanup(self.warning_context.__exit__, None, None, None)
        warnings.filterwarnings("ignore", message="Attempting to write an entirely non-virtual Dataset")

    def dataset(self, hour):
        ds = sample(hour)
        ds.temp.encoding["chunks"] = (1, 2, 17, 19)
        if self.config.rectilinear:
            lat, lon = np.meshgrid(ds.lat.values, ds.lon.values, indexing="ij")
            ds = ds.drop_vars(["lat", "lon"]).rename_dims({"lat": "y", "lon": "x"})
            ds = ds.assign_coords(Latitude=(("y", "x"), lat), Longitude=(("y", "x"), lon))
        return ds

    def build(self, hours, *, force=False, source=0, revision=0):
        files = {}
        cands = []
        for hour in hours:
            url = self.config.url_prefix + f"{hour}-{source}-{revision}"
            files[url] = self.dataset(hour)
            if revision:
                files[url]["temp"].data = files[url].temp.data + np.float32(revision)
            valid = files[url].time.values[0]
            cands.append(v.Candidate(url, source, valid - np.timedelta64(1, "h"), valid))
        self.args.force = force
        with patch.object(v, "discover", return_value=cands), patch.object(
                v, "open_files", side_effect=lambda ds, urls, workers: [files[u] for u in urls]):
            v.build(self.args, self.config, "test")

    def repo(self):
        return ic.Repository.open(self.storage, authorize_virtual_chunk_access={
            self.config.url_prefix: ic.credentials.HttpAccess})

    def assert_contents(self, hours, overviews=False, adjustments=None):
        repo = self.repo()
        store = repo.readonly_session("main").store
        expected = xr.concat([sample(h) for h in hours], dim="time")
        for i, hour in enumerate(hours):
            expected["temp"].values[i] += np.float32((adjustments or {}).get(hour, 0))
        root = xr.open_zarr(store, consolidated=False, chunks=None)
        np.testing.assert_array_equal(root.time.values, expected.time.values)
        np.testing.assert_array_equal(root.temp.values, expected.temp.values)
        np.testing.assert_array_equal(root.depth, expected.depth)
        self.assertEqual(root.attrs["virtualized_dataset"], "test")
        if overviews:
            v.validate_overviews(store, "time", expected.time.values)
            np.testing.assert_array_equal(
                xr.open_zarr(store, group="multiscale/0", consolidated=False, chunks=None).temp,
                expected.temp)
            unrounded = expected.temp.values
            for level in (1, 2):
                unrounded = v.reduce2(unrounded, "mean")
                got = xr.open_zarr(store, group=f"multiscale/{level}", consolidated=False, chunks=None)
                np.testing.assert_array_equal(got.time.values, expected.time.values)
                np.testing.assert_array_equal(got.temp.values, v.round_float32(unrounded, 12))

    def test_prepend_gaps_append_and_repeat_across_batches(self):
        self.build([3, 6])
        old = self.repo().readonly_session("main")
        with patch.object(v, "reorder_steps", wraps=v.reorder_steps) as reorder:
            self.build([0, 1, 4, 5, 7])
            self.assertEqual(reorder.call_count, 2)
        self.assert_contents([0, 1, 3, 4, 5, 6, 7])
        before = self.repo().readonly_session("main").snapshot_id
        self.build([0, 1, 4, 5, 7])
        self.assertEqual(self.repo().readonly_session("main").snapshot_id, before)
        # Different URLs bypass commit-history skipping, exercising the
        # timestamp-based duplicate check too.
        self.build([0, 1, 4, 5, 7], revision=1)
        self.assertEqual(self.repo().readonly_session("main").snapshot_id, before)
        np.testing.assert_array_equal(xr.open_zarr(old.store, consolidated=False).time.values,
                                      [sample(h).time.values[0] for h in [3, 6]])
        with patch.object(v, "reorder_steps", side_effect=AssertionError("append must not reorder")):
            self.build([8, 9])
        self.assert_contents([0, 1, 3, 4, 5, 6, 7, 8, 9])

    def test_overviews_and_force_mixed_with_insertion(self):
        self.config = dataclasses.replace(self.config,
            rectilinear={"lat": "Latitude", "lon": "Longitude"}, overviews={
            "variables": ["temp"], "levels": 3, "method": "mean", "keep_mantissa_bits": 12})
        self.build([3, 6])
        self.build([0, 3, 4, 7], force=True, revision=10)
        self.assert_contents([0, 3, 4, 6, 7], overviews=True,
                             adjustments={h: 10 for h in [0, 3, 4, 7]})
        self.assertEqual(v.stored_overviews(self.repo(), "time").keep_mantissa_bits, 12)

    def test_best_series_provenance_and_upgrades(self):
        self.config = dataclasses.replace(self.config, file_regex="", sources=(
            v.Source("nowcast", ".*"), v.Source("forecast", ".*")))
        self.build([3, 6], source=1)
        self.build([0, 3, 4], source=0)
        self.assert_contents([0, 3, 4, 6])
        store = self.repo().readonly_session("main").store
        ds = xr.open_zarr(store, consolidated=False, chunks=None)
        np.testing.assert_array_equal(ds.source_kind.values, [0, 0, 0, 1])
        np.testing.assert_array_equal(ds.forecast_reference_time.values,
                                      ds.time.values - np.timedelta64(1, "h"))
        before = self.repo().readonly_session("main").snapshot_id
        self.build([0, 3, 4, 6], source=1)
        self.assertEqual(self.repo().readonly_session("main").snapshot_id, before)

    def test_failure_is_atomic_and_retry_succeeds(self):
        self.build([3, 6])
        before = self.repo().readonly_session("main").snapshot_id
        original = v.reorder_steps
        def fail_after_reorder(*args):
            original(*args)
            raise RuntimeError("interrupted before commit")
        with patch.object(v, "reorder_steps", side_effect=fail_after_reorder):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.build([0, 4])
        self.assertEqual(self.repo().readonly_session("main").snapshot_id, before)
        self.assert_contents([3, 6])
        self.build([0, 4])
        self.assert_contents([0, 3, 4, 6])

    def test_incompatible_layout_rejected_before_replacements(self):
        self.build([3, 6])
        session = self.repo().writable_session("main")
        group = zarr.open_group(session.store, mode="r+")
        group.create_array("incompatible", shape=(2, 2), chunks=(2, 2), dtype="i4",
                           dimension_names=("time", "x"))
        session.commit("existing incompatible layout")
        before = self.repo().readonly_session("main").snapshot_id
        with patch.object(v, "replace_steps", side_effect=AssertionError("must preflight first")):
            with self.assertRaisesRegex(SystemExit, "incompatible.*chunk/shard extent"):
                self.build([0, 3], force=True)
        self.assertEqual(self.repo().readonly_session("main").snapshot_id, before)
        self.assert_contents([3, 6])

    def test_sparse_chunks_nonleading_time_axis_and_static_metadata(self):
        repo = ic.Repository.create(self.storage)
        session = repo.writable_session("main")
        g = zarr.open_group(session.store, mode="w")
        g.attrs["custom"] = "preserved"
        a = g.create_array("sparse", shape=(2, 4), chunks=(2, 1), dtype="i4", fill_value=-1,
                           dimension_names=("x", "time"))
        a[:, 0] = [10, 11]
        a[:, 2] = [30, 31]
        g.create_array("time", data=np.array([3, 6, 0, 4]), chunks=(4096,),
                       dimension_names=("time",))
        g.create_array("static", data=np.array([19, 20]), dimension_names=("x",))
        v.reorder_steps(session, "time", np.array([2, 0, 3, 1]))
        session.commit("sorted")
        g = zarr.open_group(self.repo().readonly_session("main").store, mode="r")
        np.testing.assert_array_equal(g["sparse"][:], [[30, 10, -1, -1], [31, 11, -1, -1]])
        np.testing.assert_array_equal(g["time"][:], [0, 3, 4, 6])
        np.testing.assert_array_equal(g["static"][:], [19, 20])
        self.assertEqual(g.attrs["custom"], "preserved")

    def test_virtual_references_move_without_source_reads(self):
        repo, _ = v.open_or_create_repo(self.storage, self.config)
        s = repo.writable_session("main")
        g = zarr.open_group(s.store, mode="w")
        g.create_array("a", shape=(4, 2), chunks=(1, 2), dtype="i4",
                       dimension_names=("time", "x"))
        for i in range(4):
            # These URLs cannot be read. Reordering must never dereference them.
            s.store.set_virtual_ref(f"a/c/{i}/0", self.config.url_prefix + str(i),
                                    offset=13 + i, length=8)
        async def refs(store):
            result = {}
            async for coords, kinds, paths, offsets, lengths, _ in store.array_chunk_iterator("/a"):
                for c, k, p, o, n in zip(coords, kinds, paths, offsets, lengths):
                    result[int(c[0])] = (int(k), p, int(o), int(n))
            return result
        before = asyncio.run(refs(s.store))
        order = np.array([2, 0, 3, 1])
        v.reorder_steps(s, "time", order)
        s.commit("reordered virtual chunks")
        after = asyncio.run(refs(self.repo().readonly_session("main").store))
        self.assertEqual(after, {i: before[int(j)] for i, j in enumerate(order)})
        self.assertTrue(all(r[0] == int(ic.ChunkType.virtual) for r in after.values()))

    def test_virtual_build_and_overviews_from_local_http_source(self):
        payloads = {f"/data/{h}": sample(h).temp.values.tobytes() for h in [0, 3, 4, 6, 7]}
        requests = []

        class Source(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                payload = payloads[self.path]
                bounds = self.headers["Range"].removeprefix("bytes=").split("-")
                start, end = int(bounds[0]), int(bounds[1])
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
                self.send_header("Content-Length", str(end - start + 1))
                self.end_headers()
                self.wfile.write(payload[start:end + 1])

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Source)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def close_source():
            server.shutdown()
            server.server_close()
            thread.join()
        self.addCleanup(close_source)
        self.config = dataclasses.replace(
            self.config, http_base=f"http://127.0.0.1:{server.server_port}/",
            rectilinear={"lat": "Latitude", "lon": "Longitude"}, overviews={
                "variables": ["temp"], "levels": 3, "method": "mean", "keep_mantissa_bits": 12})
        materialized = self.dataset
        metadata = zarr.create_array(
            store=zarr.storage.MemoryStore(), shape=(1, 2, 17, 19), chunks=(1, 2, 17, 19),
            dtype="f4", compressors=None, dimension_names=("time", "depth", "y", "x")).metadata
        def virtual_sample(hour):
            ds = materialized(hour)
            manifest = ChunkManifest({"0.0.0.0": {
                "path": self.config.url_prefix + str(hour), "offset": 0,
                "length": len(payloads[f"/data/{hour}"])}})
            ds["temp"].data = ManifestArray(chunkmanifest=manifest, metadata=metadata)
            return ds
        async def records(store, path):
            result = {}
            async for coords, kinds, paths, offsets, lengths, inline in store.array_chunk_iterator(path):
                for i, c in enumerate(coords):
                    result[tuple(c)] = (int(kinds[i]), paths[i], int(offsets[i]), int(lengths[i]),
                                        inline.get(i))
            return result
        with patch.object(self, "dataset", side_effect=virtual_sample):
            self.build([3, 6])
            old = self.repo().readonly_session("main")
            paths = ["/temp"] + [f"/multiscale/{k}/temp" for k in range(3)]
            before = {p: asyncio.run(records(old.store, p)) for p in paths}
            self.build([0, 4, 7])
        # Overview generation reads each added step once. Existing steps are
        # never fetched again to reorder references or overview chunks.
        self.assertCountEqual(requests, [f"/data/{h}" for h in [3, 6, 0, 4, 7]])
        store = self.repo().readonly_session("main").store
        for path in paths:
            after = asyncio.run(records(store, path))
            for coords, payload in before[path].items():
                destination = (1 if coords[0] == 0 else 3,) + coords[1:]
                self.assertEqual(after[destination], payload)
        self.assert_contents([0, 3, 4, 6, 7], overviews=True)

    def test_reject_multistep_chunks_shards_and_virtual_coordinates(self):
        for kind in ("chunk", "shard", "virtual"):
            with self.subTest(kind=kind):
                repo = ic.Repository.create(ic.in_memory_storage())
                s = repo.writable_session("main")
                g = zarr.open_group(s.store, mode="w")
                if kind == "virtual":
                    g.create_array("bad", shape=(4,), chunks=(4,), dtype="i8", dimension_names=("time",))
                    s.store.set_virtual_ref("bad/c/0", "https://example.invalid/data/0",
                                            offset=0, length=32, validate_container=False)
                else:
                    g.create_array("bad", shape=(4, 2), chunks=(1 if kind == "shard" else 2, 2),
                                   shards=(2, 2) if kind == "shard" else None,
                                   dtype="i4", dimension_names=("time", "x"))
                with self.assertRaisesRegex(SystemExit, "bad.*chunk/shard extent"):
                    v.insertion_arrays(s, "time", 4)


if __name__ == "__main__":
    unittest.main()
