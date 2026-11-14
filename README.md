# Set up

```
sbx skills add ocean-icechunks/ocean-icechunks-skills
sbx settings set claude.remoteControl true
sbx run claude --kit docker.io/sbx/mise:latest
```


# Things to investigate

## Multiscale overviews/pyramids

* https://github.com/earth-mover/icechunk-multiscales-demo
* https://github.com/carbonplan/topozarr
* https://github.com/carbonplan/ndpyramid

# Data sources

* https://registry.opendata.aws/noaa-ofs/

# virtualize.py usage

`virtualize.py` creates or updates a virtual Icechunk store over gridded
NetCDF4/HDF5 files in a public cloud bucket. It writes only Zarr metadata and
byte-range references; the data stays in the provider's files.

It is a self-contained `uv` script: dependencies are declared inline and
installed by `uv` on first run. The only requirement is `uv` (provided here by
`mise`).

## Create a store

```bash
./virtualize.py wcofs-fields-nowcast --repo ./wcofs.icechunk \
    --start 2026-10-01 --end 2026-10-31
```

`--start` and `--end` are inclusive and select the provider's daily folders.
A WCOFS nowcast folder for day D covers D-1 06:00 to D 03:00, so the store for
October starts at 2026-09-30 06:00.

## Update a store

Run the same command with a later date range:

```bash
./virtualize.py wcofs-fields-nowcast --repo ./wcofs.icechunk \
    --start 2026-11-01 --end 2026-11-14
```

Overlapping ranges are fine. Files already indexed are recorded in the commit
history and skipped without being opened, so re-running a range is a quick
no-op. New data must come after the last time already in the store;
back-filling earlier dates is refused (rebuild into a new repository instead).

## Preview without writing

```bash
./virtualize.py wcofs-fields-nowcast --start 2026-10-01 --end 2026-10-03 --dry-run
```

Lists the source files that would be indexed. No repository is needed.

## Options

| Option | Default | Meaning |
|---|---|---|
| `dataset` | — | Preset name from the config file (`wcofs-fields-nowcast`) |
| `--config` | `datasets.yml` next to the script | YAML file of dataset presets |
| `--repo` | — | Local directory, or `s3://bucket/prefix` (settings from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`; see [Local S3](#local-s3-versitygw)). Required unless `--dry-run` |
| `--start`, `--end` | — | Date range, `YYYY-MM-DD`, inclusive |
| `--batch-size` | 64 | Files per commit. A failed run resumes from the last commit |
| `--workers` | 8 | Processes opening source files in parallel |
| `--dry-run` | off | List the files and exit |
| `--no-validate` | off | Skip the final read-back check |
| `--force` | off | Re-ingest files whose time steps are already in the store, replacing their references in place instead of skipping them. New files in the range are still appended |

Every field of a dataset preset can also be overridden from the command line:
`--bucket-url`, `--http-base`, `--key-root`, `--key-template`, `--file-regex`,
`--concat-dim`, `--loadable`, `--time-chunk`, `--drop-global-attrs`,
`--coord-aliases` (e.g. `'lat=lat_rho,lon=lon_rho'`; `''` for none). Run
`./virtualize.py --help` for the full list.

## Adding a dataset

Add an entry to `datasets.yml` (next to `virtualize.py`), keyed by preset name,
or put it in your own file and pass `--config path/to/file.yml`:

| Field | Example (WCOFS) | Meaning |
|---|---|---|
| `bucket_url` | `s3://noaa-nos-ofs-pds` | Bucket used for listing (anonymous) |
| `http_base` | `https://noaa-nos-ofs-pds.s3.amazonaws.com/` | Public HTTPS base embedded in the references; ends in `/` |
| `key_root` | `wcofs/netcdf/` | Dataset prefix inside the bucket; ends in `/` |
| `key_template` | `%Y/%m/%d/` | Per-day folder under `key_root` (`strftime` format) |
| `file_regex` | `^wcofs\.t\d{2}z\.\d{8}\.fields\.n\d{3}\.nc$` | Which file names in a folder belong to the stream |
| `concat_dim` | `ocean_time` | Dimension the files are appended along |
| `loadable` | `[ocean_time]` | Small variables to materialize; must include `concat_dim` |
| `time_chunk` | 4096 | Chunk length of the materialized time coordinate |
| `drop_global_attrs` | `[file, history, rst_file, ..., 'frc_file_[0-9]*']` | Global attributes that describe a single run/file, as a list of names. Wildcards allowed (see below). Default `[file, history]` |
| `coord_aliases` | `{lat: lat_rho, lon: lon_rho}` | Extra coordinates `{alias: source}` that reuse the source coordinate's references (no data copied), for viewers that only recognise names like `lat`/`lon`. Default none |

The WCOFS entry in full:

```yaml
wcofs-fields-nowcast:
  bucket_url: s3://noaa-nos-ofs-pds
  http_base: https://noaa-nos-ofs-pds.s3.amazonaws.com/
  key_root: wcofs/netcdf/
  key_template: "%Y/%m/%d/"
  file_regex: '^wcofs\.t\d{2}z\.\d{8}\.fields\.n\d{3}\.nc$'
  concat_dim: ocean_time
  loadable: [ocean_time]
  coord_aliases: {lat: lat_rho, lon: lon_rho}
  drop_global_attrs:
    - file
    - history
    - rst_file
    - his_base
    - sta_file
    - ini_file
    - river_file
    - 'frc_file_[0-9]*'   # frc_file_01, frc_file_02, ...
    - 'bry_file_[0-9]*'
    - 'clm_file_[0-9]*'
```

`drop_global_attrs` entries are exact attribute names, or shell-style patterns
using `*` (any characters), `?` (one character) and `[...]` (one character from
a set). The whole name must match: `history` drops `history` but not
`history_old`. `loadable` and `drop_global_attrs` must be YAML lists, even
with a single entry.

Quoting in YAML: put single quotes around regexes (`file_regex`) so
backslashes are kept, and around list entries that start with `*` or `[`,
which YAML would otherwise read as an alias or a nested list. Patterns with
`[...]` inside an inline list (`[a, 'b_[0-9]*']`) also need quotes. The
example quotes all patterns to be safe.

On the command line, `--loadable` and `--drop-global-attrs` take
comma-separated values and replace the preset's list:

```bash
./virtualize.py wcofs-fields-nowcast --start 2026-10-01 --end 2026-10-01 --dry-run \
    --drop-global-attrs 'file,history,*_file_[0-9]*'
```

All files in a stream must share variables, dimensions, dtypes and chunking,
and each time step must appear in exactly one file. The script stops if either
assumption is violated. That is why WCOFS forecast files (`fNNN`), which
overlap from day to day, are not a preset.

## Reading the store

```python
import icechunk as ic
import xarray as xr
import zarr

zarr.config.set({"async.concurrency": 128})

prefix = "https://noaa-nos-ofs-pds.s3.amazonaws.com/wcofs/netcdf/"
repo = ic.Repository.open(
    ic.local_filesystem_storage("./wcofs.icechunk"),
    authorize_virtual_chunk_access={prefix: ic.credentials.HttpAccess},
)
ds = xr.open_zarr(repo.readonly_session("main").store, consolidated=False, chunks={})
```

The source prefix is also stored in the dataset attribute `virtualized_source`.
Reading data needs internet access to the source bucket; no AWS credentials are
required.

## Local S3 (versitygw)

`compose.yml` runs [versitygw](https://github.com/versity/versitygw) on
`http://localhost:7070` with a posix backend. `./repos` is the bucket
`icechunk`, so `s3://icechunk/foo.icechunk` is stored in `./repos/foo.icechunk`.

- **Writes** need the keys from `.env`.
- **Reads** are anonymous: a bucket policy allows `GetObject` and `ListBucket`
  for everyone. CORS allows any origin, with `Range` requests and
  `Content-Range` exposed, so browser tools such as gridlook can read the
  repositories.

```bash
cp .env.template .env      # then set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
docker compose up -d       # s3-init applies the public-read policy and CORS, then exits

./virtualize.py wcofs-fields-nowcast --repo s3://icechunk/wcofs.icechunk \
    --start 2026-10-01 --end 2026-10-31
```

`.env` is read by `docker compose` (the keys become versitygw's root account)
and by `virtualize.py` (for `s3://` destinations). Variables already set in
the shell take precedence. Leave `AWS_ENDPOINT_URL` empty to target AWS
itself. The anonymous source listing and file reads ignore all `AWS_*`
variables, so these settings only affect the destination.

Public URL of a repository: `http://localhost:7070/icechunk/<name>.icechunk/`.
Reading it from Python without credentials:

```python
storage = ic.s3_storage(bucket="icechunk", prefix="wcofs.icechunk",
    endpoint_url="http://localhost:7070", allow_http=True,
    force_path_style=True, anonymous=True, region="us-east-1")
```

**Write repositories through S3, not straight into `./repos`.** versitygw
stores each object's ETag as an extended attribute when the object is
uploaded. Files written directly to `./repos` (for example with
`--repo ./repos/foo.icechunk`) have no ETag, and Icechunk refuses to open
them over S3 (`Object should have an etag`). To publish such a repository,
upload it, e.g. `aws s3 sync --endpoint-url http://localhost:7070
./some.icechunk s3://icechunk/some.icechunk` from a directory outside
`./repos`.

## Viewing in gridlook

`docker compose up -d` also builds and starts
[gridlook](https://github.com/d70-t/gridlook) on `http://localhost:3000`, from a
pinned commit plus `gridlook/coordinates-by-units.patch`. Open a repository
with `http://localhost:3000/#http://localhost:7070/icechunk/<name>.icechunk`.

ROMS output such as WCOFS has four staggered grids (`lat_rho`, `lat_u`,
`lat_v`, `lat_psi`, ...). Stock gridlook only recognises coordinates named
`lat`/`lon` in a variable's `coordinates` attribute. Otherwise it takes any
array with `degree_north` units, which here is `lat_psi` (1015×347). It then
draws 348-wide rho-grid fields such as `temp` on a 347-wide grid, so every
row shifts by one cell and the land mask shows up as diagonal bands. Two fixes
address this:

- The patch makes gridlook recognise `coordinates` entries by their units, so
  each variable gets its own grid (rho, u or v).
- `virtualize.py` adds `lat`/`lon` aliases of `lat_rho`/`lon_rho`
  (`coord_aliases`), so unpatched gridlook instances get rho-grid fields right.

`virtualize.py` also adds a numeric `missing_value` next to each base64
`_FillValue` (the Zarr v3 form only xarray decodes), so land cells (1e37) are
masked outside xarray too. Both repairs apply only to arrays a build creates.
Repositories built before them must be rebuilt to pick them up.

## Notes

- The local-filesystem repository is not safe for concurrent writers: run one
  build at a time per repository.
- The `s3://` destination has been tested against the local versitygw
  container (create, append, re-run). It has not been tested against AWS itself.
- Opening source files runs at roughly one file per second overall; a month of
  WCOFS nowcast fields (248 files) takes about 4 minutes.
