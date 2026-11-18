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
"""Run with uv run --script test_bitrounding.py."""
import argparse
import dataclasses
import tempfile
import unittest
import warnings

import icechunk as ic
import numpy as np
import xarray as xr
import zarr

import virtualize as v


class RoundingTests(unittest.TestCase):
    def test_against_float64_rounding_oracle(self):
        rng = np.random.default_rng(3)
        a = np.ldexp(rng.uniform(-1, 1, 10000), rng.integers(-120, 121, 10000)).astype("f4")
        mantissa, exponent = np.frexp(a.astype("f8"))
        # Subnormals share the smallest normal exponent's quantization grid.
        exponent = np.maximum(exponent, -125)
        mantissa = np.ldexp(a.astype("f8"), -exponent)
        for keep in range(24):
            expected = np.ldexp(np.rint(np.ldexp(mantissa, keep + 1)), exponent - keep - 1).astype("f4")
            np.testing.assert_array_equal(v.round_float32(a, keep), expected)

    def test_ties_carry_and_sign(self):
        # At one retained bit, 1.25 ties downward and 1.75 ties upward to 2.
        a = np.array([1.25, 1.75, -1.25, -1.75, 1.26, 1.24], dtype="f4")
        np.testing.assert_array_equal(v.round_float32(a, 1), [1, 2, -1, -2, 1.5, 1])
        np.testing.assert_array_equal(a, np.array([1.25, 1.75, -1.25, -1.75, 1.26, 1.24], dtype="f4"))

    def test_special_values_subnormals_and_overflow(self):
        words = np.array([0, 0x80000000, 0x7f800000, 0xff800000,
                          0x7fc00000, 0x7f800001, 0x7fffffff, 0xff800001], dtype="u4")
        for keep in range(24):
            np.testing.assert_array_equal(v.round_float32(words.view("f4"), keep).view("u4"), words)
        # Smallest subnormal rounds to zero; largest promotes to smallest normal.
        a = np.array([1, 0x007fffff, 0x7f7fffff, 0xff7fffff], dtype="u4").view("f4")
        expected = np.array([0, 0x00800000, 0x7f800000, 0xff800000], dtype="u4")
        np.testing.assert_array_equal(v.round_float32(a, 10).view("u4"), expected)

    def test_noop_noncontiguous_and_bounds(self):
        rng = np.random.default_rng(42)
        a = rng.normal(size=(16, 18)).astype("f4")[:, ::2]
        b = v.round_float32(a, 23)
        np.testing.assert_array_equal(a.view("u4"), b.view("u4"))
        self.assertFalse(np.shares_memory(a, b))
        for keep in [0, 10, 12, 16]:
            b = v.round_float32(a, keep)
            self.assertTrue(np.all(np.abs(b.astype("f8") - a) <= np.abs(a) * 2.0 ** (-keep - 1)))
        for invalid in [-1, 24, True, 10.5, "10"]:
            with self.assertRaises(SystemExit):
                v.parse_keep_mantissa_bits(invalid)
        with self.assertRaises(ValueError):
            v.round_float32(a.astype("f8"), 10)

    def test_presets_and_resolution(self):
        ds = v.load_datasets()["wcofs-regulargrid-nowcast"]
        args = argparse.Namespace(overviews=None, overview_variables=None, overview_levels=None,
                                  overview_method=None, overview_keep_mantissa_bits=0,
                                  overview_keep_mantissa_bits_by_variable=None)
        self.assertEqual(v.resolve_overviews(args, ds, False, None).keep_mantissa_bits, 0)
        stored = v.Overviews(("temp",), 3, "mean", 12)
        with self.assertLogs(v.log, level="WARNING"):
            self.assertEqual(v.resolve_overviews(args, ds, True, stored), stored)
        args.overview_keep_mantissa_bits = None
        ds = dataclasses.replace(ds, overviews={"keep_mantissa_bits": 16})
        self.assertEqual(v.resolve_overviews(args, ds, False, None).keep_mantissa_bits, 16)
        ds = dataclasses.replace(ds, overviews={"keep_mantissa_bits": 12,
            "keep_mantissa_bits_by_variable": {"temp": 16, "salt": None}})
        args.overview_keep_mantissa_bits = 10
        args.overview_keep_mantissa_bits_by_variable = [v.parse_variable_rounding_arg("temp=14"),
                                                       v.parse_variable_rounding_arg("temp=0")]
        ov = v.resolve_overviews(args, ds, False, None)
        self.assertEqual([ov.bits_for(n) for n in ["temp", "salt", "zeta"]], [0, None, 10])
        with self.assertLogs(v.log, level="WARNING"):
            self.assertEqual(v.resolve_overviews(args, ds, True, stored), stored)
        for invalid in [-1, 24, True, 10.5, "10"]:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml") as f:
                f.write(f'demo:\n  file_regex: "x"\n  overviews:\n    keep_mantissa_bits: {invalid!r}\n')
                f.flush()
                with self.assertRaises(SystemExit):
                    v.load_datasets(v.Path(f.name))

    def test_variable_rounding_config(self):
        for invalid in [None, [], 12, {"": 10}, {1: 10}, {"temp": True}, {"temp": 24}]:
            with self.subTest(invalid=invalid), self.assertRaises(SystemExit):
                v.parse_variable_rounding(invalid)
        for invalid in ["temp", "=10", "temp=24", "temp=-1", "temp=true", "temp=1.5", "temp="]:
            with self.subTest(invalid=invalid), self.assertRaises(argparse.ArgumentTypeError):
                v.parse_variable_rounding_arg(invalid)
        self.assertEqual(v.parse_variable_rounding_arg("temp=none"), ("temp", None))
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml") as f:
            f.write('demo:\n  bucket_url: s3://test\n  http_base: https://example.com/\n'
                    '  key_root: data/\n  key_template: "%Y/"\n  concat_dim: time\n'
                    '  file_regex: "x"\n  overviews:\n    keep_mantissa_bits: 12\n'
                    '    keep_mantissa_bits_by_variable: {temp: 16, salt: null}\n')
            f.flush()
            cfg = v.load_datasets(v.Path(f.name))["demo"].overviews
            self.assertEqual(cfg["keep_mantissa_bits_by_variable"], {"temp": 16, "salt": None})


def sample(offset=0):
    rng = np.random.default_rng(17 + offset)
    a = rng.normal(size=(1, 2, 17, 19)).astype("f4")
    a[..., :4, :4] = np.nan
    ds = xr.Dataset({"temp": (("time", "depth", "lat", "lon"), a)}, coords={
        "time": np.array(["2026-11-01"], dtype="datetime64[ns]") + np.timedelta64(offset, "h"),
        "depth": [0, 10], "lat": np.arange(17, dtype="f8"), "lon": np.arange(19, dtype="f8")})
    ds.time.encoding.update(units="hours since 2026-11-01", dtype="int64")
    return ds


class RepositoryTests(unittest.TestCase):
    def setUp(self):
        self.warnings = warnings.catch_warnings()
        self.warnings.__enter__()
        # These synthetic fixtures intentionally materialize level 0. Real
        # source references are checked by the remote-data benchmark.
        warnings.filterwarnings("ignore", message="Attempting to write an entirely non-virtual Dataset")

    def tearDown(self):
        self.warnings.__exit__(None, None, None)

    def test_lifecycle_and_unrounded_chain(self):
        for keep in [None, 0, 12, 23]:
            with self.subTest(keep=keep), tempfile.TemporaryDirectory() as path:
                storage = ic.local_filesystem_storage(path)
                repo = ic.Repository.create(storage)
                ov = v.Overviews(("temp",), 3, "mean", keep)
                for index, (offset, replace) in enumerate([(0, False), (1, False), (2, True)]):
                    ds = sample(offset)
                    session = repo.writable_session("main")
                    v.update_overviews(session, ds, ov, "time", 0 if replace else index,
                                       create=index == 0, replace=replace)
                    session.commit("sample")
                    repo = ic.Repository.open(storage)
                    self.assertEqual(v.stored_overviews(repo, "time"), ov)
                    store = repo.readonly_session("main").store
                    level0 = xr.open_zarr(store, group="multiscale/0", consolidated=False, chunks=None)
                    expected_times = [sample(0).time.values[0]]
                    if index > 0:
                        expected_times.append(sample(1).time.values[0])
                    np.testing.assert_array_equal(level0.time.values, expected_times)
                    v.validate_overviews(store, "time", level0.time.values)
                    expected = ds.temp.values
                    for level in [1, 2]:
                        expected = v.reduce2(expected, "mean")
                        rounded = v.round_float32(expected, keep) if keep is not None else expected
                        got = xr.open_zarr(store, group=f"multiscale/{level}", consolidated=False,
                                           chunks=None).temp.isel(time=slice(0, 1) if replace else slice(index, index + 1))
                        np.testing.assert_array_equal(got.values, rounded)
                    # Source and coordinates have never been rounded.
                    got = level0.temp.isel(time=slice(0, 1) if replace else slice(index, index + 1))
                    np.testing.assert_array_equal(got.values, ds.temp.values)
                    np.testing.assert_array_equal(level0.lat, ds.lat)
                    root = zarr.open_group(store, path="multiscale", mode="r")
                    self.assertEqual("bitround" in root.attrs, keep is not None)

    def test_reject_unsupported_dtype_before_writing(self):
        ds = sample().astype("f8")
        with self.assertRaisesRegex(SystemExit, "float32"):
            v.overview_variables(ds, v.Overviews(("temp",), keep_mantissa_bits=10), "time")
        ov = v.Overviews(("temp",), keep_mantissa_bits=10, keep_mantissa_bits_by_variable={"temp": None})
        self.assertEqual(v.overview_variables(ds, ov, "time"), ["temp"])
        with self.assertRaisesRegex(SystemExit, "ineligible"):
            v.overview_variables(ds, dataclasses.replace(ov, keep_mantissa_bits_by_variable={"typo": 12}), "time")

    def test_variable_policies_lifecycle(self):
        for default in [None, 12]:
            with self.subTest(default=default), tempfile.TemporaryDirectory() as path:
                storage = ic.local_filesystem_storage(path)
                repo = ic.Repository.create(storage)
                ov = v.Overviews(("salt", "temp", "zeta"), 3, "mean", default,
                                 {"temp": 0, "zeta": None})
                for index, (offset, replace) in enumerate([(0, False), (1, False), (2, True)]):
                    ds = sample(offset)
                    ds["salt"] = ds.temp * np.float32(32)
                    ds["zeta"] = ds.temp.astype("f8")
                    session = repo.writable_session("main")
                    v.update_overviews(session, ds, ov, "time", 0 if replace else index,
                                       create=index == 0, replace=replace)
                    session.commit("mixed rounding policies")
                    repo = ic.Repository.open(storage)
                    self.assertEqual(v.stored_overviews(repo, "time"), ov)
                    ov = v.stored_overviews(repo, "time")
                    store = repo.readonly_session("main").store
                    times = xr.open_zarr(store, group="multiscale/0", consolidated=False, chunks=None).time.values
                    v.validate_overviews(store, "time", times)
                    for var in ov.variables:
                        expected = ds[var].values
                        for level in [1, 2]:
                            expected = v.reduce2(expected, "mean")
                            bits = ov.bits_for(var)
                            rounded = v.round_float32(expected, bits) if bits is not None else expected
                            got = xr.open_zarr(store, group=f"multiscale/{level}", consolidated=False,
                                               chunks=None)[var].isel(time=slice(0, 1) if replace else slice(index, index + 1))
                            np.testing.assert_array_equal(got.values, rounded)
                session = repo.writable_session("main")
                array = zarr.open_array(session.store, path="multiscale/1/temp", mode="r+")
                array[-1, 0, 4, 4] += np.float32(0.125)
                session.commit("corrupt overridden variable")
                with self.assertRaisesRegex(AssertionError, "temp"):
                    v.validate_overviews(repo.readonly_session("main").store, "time", times)

    def test_unknown_algorithm(self):
        group = zarr.group(store=zarr.storage.MemoryStore())
        self.assertEqual(v.stored_rounding(group), (None, {}))
        # Scalar metadata from stores built before variable overrides still reads.
        group.attrs["bitround"] = {"algorithm": v.BITROUND_ALGORITHM, "keep_mantissa_bits": 12}
        self.assertEqual(v.stored_rounding(group), (12, {}))
        group.attrs["bitround"] = {"algorithm": "future", "keep_mantissa_bits": 12}
        with self.assertRaisesRegex(SystemExit, "unsupported"):
            v.stored_rounding(group)

    def test_other_reductions(self):
        for method in ["min", "max", "nearest"]:
            with self.subTest(method=method):
                repo = ic.Repository.create(ic.in_memory_storage())
                session = repo.writable_session("main")
                ds = sample()
                v.update_overviews(session, ds, v.Overviews(("temp",), 3, method, 10),
                                   "time", 0, create=True)
                session.commit("sample")
                v.validate_overviews(repo.readonly_session("main").store, "time", ds.time.values)


if __name__ == "__main__":
    unittest.main()
