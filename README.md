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
| `dataset` | — | Preset name from the config file (`wcofs-fields-nowcast`, `ciofs-fields-nowcast`) |
| `--config` | `datasets.yml` next to the script | YAML file of dataset presets |
| `--repo` | — | Local directory, or `s3://bucket/prefix` (settings from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`; see [Local S3](#local-s3-versitygw)). Required unless `--dry-run` |
| `--start`, `--end` | — | Date range, `YYYY-MM-DD`, inclusive |
| `--batch-size` | 64 | Files per commit. A failed run resumes from the last commit |
| `--workers` | 8 | Processes opening source files in parallel |
| `--dry-run` | off | List the files and exit |
| `--no-validate` | off | Skip the final read-back check |
| `--force` | off | Re-ingest files whose time steps are already in the store, replacing their references in place instead of skipping them. New files in the range are still appended |
| `--no-catalog` | off | Don't update the gridlook/Xreds catalogs after the build (see [Catalogs](#catalogs)) |
| `--scan-catalogs ROOT` | — | Only rebuild both catalogs from every repository under `ROOT`, then exit |
| `--public-base-url` | see [Catalogs](#catalogs) | Browser URL of the repository root, used for gridlook entries |
| `--xreds-base-url` | see [Catalogs](#catalogs) | `s3://` URL of the repository root as Xreds sees it |
| `--xreds-endpoint-url` | see [Catalogs](#catalogs) | S3 endpoint written into Xreds entries (`''` for AWS) |

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
pinned commit plus `patch/gridlook/coordinates-by-units.patch`. Open a repository
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

## Catalogs

`virtualize.py` keeps two catalog files in a `_catalogs/` directory in the
repository root, next to the `*.icechunk` repositories (`s3://icechunk/_catalogs/`
for `--repo s3://icechunk/wcofs.icechunk`). On the local S3 server they are
public too:

| File | For | Public URL (local S3) |
|---|---|---|
| `_catalogs/gridlook-catalog.json` | [gridlook catalog](https://github.com/d70-t/gridlook/blob/main/docs/catalogs.md) | `http://localhost:7070/icechunk/_catalogs/gridlook-catalog.json` |
| `_catalogs/xreds-datasets.yml` | Xreds `DATASETS_MAPPING_FILE` | `s3://icechunk/_catalogs/xreds-datasets.yml` |

**After every build** (not `--dry-run`, not `--no-catalog`), the entry for
the repository just built is created or replaced in both files. Other entries,
including ones added by hand, and the gridlook catalog title are left as they
are. Entries are keyed by repository name (`wcofs.icechunk` becomes the Xreds
id `wcofs`). They are built from the repository itself (its title, time range
and source prefix), not from the preset.

**To rebuild both catalogs from scratch**, run scan mode. It needs no dataset,
dates or `--repo`. It lists the `*.icechunk` repositories directly under the
root, rewrites both files, and exits:

```bash
./virtualize.py --scan-catalogs s3://icechunk
./virtualize.py --scan-catalogs ./repos        # local root (no S3 write needed)
```

The URLs written into entries default to the compose setup:

- gridlook `url`: for an `s3://` root, `$AWS_ENDPOINT_URL/<bucket>/<prefix>/`
  (`https://<bucket>.s3.amazonaws.com/<prefix>/` without an endpoint); for a
  local root, `http://localhost:7070/icechunk/`. Override with
  `--public-base-url`.
- Xreds `path`: the `s3://` root itself, or `s3://icechunk/` for a local
  root. Override with `--xreds-base-url`.
- Xreds `endpoint_url`: `http://s3:7070` (the S3 service as seen from inside
  compose) when `AWS_ENDPOINT_URL` is set or the root is local; none (AWS)
  otherwise. Override with `--xreds-endpoint-url` (`''` for AWS).

The gridlook service mounts `./repos/_catalogs` and serves its catalog as the
default at `http://localhost:3000/`; it is re-read on every page load. It can
also be opened from S3 with
`http://localhost:3000/#http://localhost:7070/icechunk/_catalogs/gridlook-catalog.json`.

## Serving with Xreds

`docker compose up -d` also builds and starts
[Xreds](https://github.com/asascience-open/xreds) on `http://localhost:8090`,
from a pinned commit plus `patch/xreds/virtual-icechunk-s3-http.patch`. The image
follows upstream's `Dockerfile.viewer`: the bundled React viewer is served at
`http://localhost:8090/`, the API at `/datasets/...` and its docs at `/docs`. It reads its
catalog from `./repos/_catalogs/xreds-datasets.yml`, mounted into the container
(Xreds can also read it from S3 or HTTP: any fsspec URL, e.g.
`s3://icechunk/_catalogs/xreds-datasets.yml`).

Stock Xreds can't open these repositories. Its `virtual-icechunk` loader
gives no way to set an S3 endpoint, and it only authorizes S3 chunk
containers, while these repositories reference public HTTPS files. The patch
changes only that loader:

- `storage_options` may set `endpoint_url`, `allow_http` and
  `force_path_style`, which are passed to `icechunk.s3_storage`.
- `virtual_chunk_container: {type: http, store: {path: <prefix>}}` authorizes
  an HTTP(S) chunk container (anonymous). The container itself comes from the
  repository's stored config.

Xreds reads the catalog only at startup. It fails to start if the catalog
does not exist yet. After a build adds a new repository, run `docker compose
restart xreds`.

Two settings are added to an Xreds entry when the repository needs them,
judged from the repository's own variables:

- **`drop_variables`: alias coordinates.** Coordinates added by
  `coord_aliases` (recognised by their `alias of ...` comment, e.g. `lat`,
  `lon`) are hidden from Xreds. They copy their source's CF attributes, so
  cf_xarray would find two latitudes and WMS would fail. Gridlook still sees
  them.
- **`extensions: {roms: {}}` for ROMS output.** This applies when the
  repository has `angle`, `lat_rho`, `lon_rho`, `u`, `v`, `temp` or `salt`,
  and the ROMS rho/u/v and `s_rho` dimensions. The extension adds `u_rotated`
  and `v_rotated`, currents rotated to east/north on the rho grid. Like
  upstream's ROMS examples, `dstart` is also dropped.

The NOAA `vdatum` extension, which needs a separate vdatum dataset, is not
configured.

## Notes

- The local-filesystem repository is not safe for concurrent writers: run one
  build at a time per repository.
- The `s3://` destination has been tested against the local versitygw
  container (create, append, re-run). It has not been tested against AWS itself.
- Opening source files runs at roughly one file per second overall; a month of
  WCOFS nowcast fields (248 files) takes about 4 minutes.
