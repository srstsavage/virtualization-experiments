# Set up

```
sbx skills add ocean-icechunks/ocean-icechunks-skills
sbx settings set claude.remoteControl true
sbx run claude --kit docker.io/sbx/mise:latest
```


# Things to investigate

## Multiscale overviews/pyramids

Implemented for regular-grid datasets: see [Multiscale overviews](#multiscale-overviews).
Background reading:

* https://github.com/earth-mover/icechunk-multiscales-demo
* https://github.com/carbonplan/topozarr
* https://github.com/carbonplan/ndpyramid

# Data sources

* https://registry.opendata.aws/noaa-ofs/ (https://tidesandcurrents.noaa.gov/ofs/ofs_faq.html)

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

`--start` and `--end` are inclusive and select by the data's valid time (the
concat coordinate), in UTC. Each is a date (`2026-10-01`) or a datetime
(`2026-10-01T06:00`, `2026-10-01 06:00`; a `Z` suffix is fine, other UTC
offsets are refused). A date alone as `--end` covers that whole day, up to
00:00 of the next, so the command above indexes 2026-10-01 00:00 to
2026-10-31 21:00 (WCOFS is 3-hourly).

The provider files data by day folder, and a folder does not hold exactly that
day's data (a WCOFS nowcast folder for day D covers D-1 06:00 to D 03:00), so
the script also lists `folder_margin_days` (default 1) folders beyond each end of
the range, opens the candidates, and keeps only the files whose valid time is in
the range.

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

## Best time series (nowcast + forecast)

The `wcofs-regulargrid-best` and `ciofs-regulargrid-best` presets hold one
"best" file per time step: nowcast data up to the latest model cycle, then the
newest forecast up to the end of the forecast (72 h WCOFS, 48 h CIOFS). Each
step is filled from the best file published so far:

**nowcast > newer forecast > older forecast**

Keep such a store current by re-running it on a schedule (e.g. hourly cron)
with an open end:

```bash
./virtualize.py wcofs-regulargrid-best --repo s3://icechunk/wcofs-rg-best.icechunk \
    --start today-1
```

Each run works out the best file for every valid time from the file names
alone, without opening anything. It then opens only the files that add a new
step (appended) or beat the file the store holds for that step (references,
and overview levels, replaced in place). When no new cycle has appeared, a run
does nothing and makes no commit. `--force` re-ingests the chosen files even
when they rank the same. `--start today-N` / `today` and an omitted `--end`
exist for this use; `--end` is required only for plain presets.

Each step's origin is stored in two small, materialized coordinates:
`source_kind` (0 = nowcast, 1 = forecast; `flag_meanings`) and
`forecast_reference_time` (the model cycle). Use them to tell forecast steps
from nowcast ones, e.g. `ds.time[ds.source_kind == 1]`. The commit metadata
records `upgraded` entries (file, and what it replaced). Xreds entries hide
both coordinates.

How it is configured: the preset has `sources` instead of `file_regex` (see
[Adding a dataset](#adding-a-dataset)). Each source is a regex with named
groups for the cycle date and hour and the lead time; the file's valid time,
cycle + lead + `lead_offset_hours`, is checked against the file when it is
opened. Steps that fall inside the store but are not in it (a different time
step, an old gap) are refused, as for appends.

Replacing steps leaves superseded data behind in old snapshots: chiefly
overview chunks, about 48 CIOFS steps per cycle. So after every run that
commits, snapshots older than `--retain` (default `3d`) are expired and
anything only they used is garbage-collected. `--retain 0` keeps only the
latest snapshot; use it only when nothing else writes to the repository at the
same time. `--no-gc` turns this off. Expired commits also lose the record of
which files they indexed, so a plain preset re-run over an old window reopens
those files (metadata only) before skipping them by their times.

## Preview without writing

```bash
./virtualize.py wcofs-fields-nowcast --start 2026-10-01 --end 2026-10-03 --dry-run
```

Lists the source files that would be indexed, with their valid times. The
candidate files are opened (metadata only) to read those times. No repository
is needed. For `sources` presets the valid times come from the names, so
nothing is opened. The best file per step is listed with its kind and cycle,
without comparing against a repository.

## Options

| Option | Default | Meaning |
|---|---|---|
| `dataset` | — | Preset name from the config file (`wcofs-fields-nowcast`, `ciofs-fields-nowcast`, `wcofs-regulargrid-nowcast`, `ciofs-regulargrid-nowcast`, `wcofs-regulargrid-best`, `ciofs-regulargrid-best`) |
| `--config` | `datasets.yml` next to the script | YAML file of dataset presets |
| `--repo` | — | Local directory, or `s3://bucket/prefix` (settings from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`; see [Local S3](#local-s3-versitygw)). Required unless `--dry-run` |
| `--start`, `--end` | — | Valid-time range, UTC, inclusive: `YYYY-MM-DD`, `YYYY-MM-DDTHH[:MM[:SS]]`, or `today` / `today-N`. A date-only `--end` includes its whole day. `--end` may be omitted for [best time series](#best-time-series-nowcast--forecast) presets (everything published) |
| `--folder-margin-days` | 1 (or preset) | Day folders listed beyond each end of the range, for data filed under a neighbouring day |
| `--batch-size` | 64 | Files per commit. A failed run resumes from the last commit |
| `--workers` | 8 | Processes opening source files in parallel |
| `--dry-run` | off | List the files and exit |
| `--no-validate` | off | Skip the final read-back check |
| `--force` | off | Re-ingest files whose time steps are already in the store, replacing their references in place instead of skipping them. New files in the range are still appended |
| `--retain` | `3d` | After a build that committed, expire snapshots older than this (`3d`, `12h`, `30m`, `0`) and garbage-collect what only they used |
| `--no-gc` | off | Keep all snapshots and data |
| `--no-catalog` | off | Don't update the gridlook/Xreds catalogs after the build (see [Catalogs](#catalogs)) |
| `--scan-catalogs ROOT` | — | Only rebuild both catalogs from every repository under `ROOT`, then exit |
| `--public-base-url` | see [Catalogs](#catalogs) | Browser URL of the repository root, used for gridlook entries |
| `--xreds-base-url` | see [Catalogs](#catalogs) | `s3://` URL of the repository root as Xreds sees it |
| `--xreds-endpoint-url` | see [Catalogs](#catalogs) | S3 endpoint written into Xreds entries (`''` for AWS) |
| `--overviews` / `--no-overviews` | preset | Build [multiscale overviews](#multiscale-overviews) in a new repository (default: when the preset has an `overviews` entry) |
| `--overview-variables` | preset | Comma-separated variables to build overviews for |
| `--overview-levels` | 3 | Levels including full resolution (`auto`: halve until a side would drop below 256) |
| `--overview-method` | `mean` | `mean`, `min`, `max` or `nearest` |

Every field of a dataset preset can also be overridden from the command line:
`--bucket-url`, `--http-base`, `--key-root`, `--key-template`, `--file-regex`,
`--concat-dim`, `--loadable`, `--time-chunk`, `--drop-global-attrs`,
`--coord-aliases` (e.g. `'lat=lat_rho,lon=lon_rho'`; `''` for none),
`--rectilinear` (e.g. `'lat=Latitude,lon=Longitude'`; `''` for none). Run
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
| `file_regex` | `^wcofs\.t\d{2}z\.\d{8}\.fields\.n\d{3}\.nc$` | Which file names in a folder belong to the stream (one step per file). Either this or `sources` |
| `sources` | (`*-best`: nowcast, forecast) | [Best time series](#best-time-series-nowcast--forecast) over ranked streams, most preferred first: a list of `{kind, file_regex, lead_offset_hours}`. Each `file_regex` has the named groups `date` (YYYYMMDD) and `hour` (HH) of the cycle and `lead` (hours); valid time = cycle + lead + `lead_offset_hours` (default 0, e.g. `-24` for WCOFS nowcasts) |
| `concat_dim` | `ocean_time` | Dimension the files are appended along |
| `loadable` | `[ocean_time]` | Small variables to materialize; must include `concat_dim` |
| `time_chunk` | 4096 | Chunk length of the materialized time coordinate |
| `drop_global_attrs` | `[file, history, rst_file, ..., 'frc_file_[0-9]*']` | Global attributes that describe a single run/file, as a list of names. Wildcards allowed (see below). Default `[file, history]` |
| `coord_aliases` | `{lat: lat_rho, lon: lon_rho}` | Extra coordinates `{alias: source}` that reuse the source coordinate's references (no data copied), for viewers that only recognise names like `lat`/`lon`. Default none |
| `rectilinear` | (regulargrid: `{lat: Latitude, lon: Longitude}`) | A regular grid that the files store as 2-D lat/lon arrays. They must be in `loadable`; they are checked to be rectilinear and evenly spaced, then replaced by 1-D `lat`/`lon` coordinates, and their dimensions are renamed `lat`/`lon` (references unchanged). Default none |
| `overviews` | (regulargrid: `{variables: [zeta, temp, ...], levels: 3, method: mean}`) | Build [multiscale overviews](#multiscale-overviews) by default. Needs a regular grid. Default none |

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

Backblaze B2 rejects conditional write headers (`If-Match` / `If-None-Match`).
For S3 destinations with `AWS_ENDPOINT_URL` hosts ending in `.backblazeb2.com`,
the script automatically disables conditional writes and defaults
`AWS_RESPONSE_CHECKSUM_VALIDATION` to `WHEN_REQUIRED` to handle B2's checksum
headers on empty `304 Not Modified` responses. Explicit values in the environment
or `.env` take precedence for checksum validation:

```bash
./virtualize.py ciofs-regulargrid-best \
    --repo s3://grid-virtualization/ciofs-rg-best.icechunk --start today-1
```

This saves `unsafe_use_conditional_create=False` and
`unsafe_use_conditional_update=False` in the repository configuration. Allow
only one writer at a time, including builds and garbage collection, because
concurrent updates can overwrite each other. Subsequent runs retain this setting.
For other services that reject conditional writes, pass
`--unsafe-no-conditional-writes` explicitly.
If an earlier creation attempt left objects behind and retrying reports
`bad object store prefix`, use a fresh empty repository prefix.

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
| `_catalogs/gridlook.json` | [gridlook catalog](https://github.com/d70-t/gridlook/blob/main/docs/catalogs.md) | `http://localhost:7070/icechunk/_catalogs/gridlook.json` |
| `_catalogs/xreds.yml` | Xreds `DATASETS_MAPPING_FILE` | `s3://icechunk/_catalogs/xreds.yml` |

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
`http://localhost:3000/#http://localhost:7070/icechunk/_catalogs/gridlook.json`.

## Serving with Xreds

`docker compose up -d` also builds and starts
[Xreds](https://github.com/asascience-open/xreds) on `http://localhost:8090`,
from a pinned commit plus `patch/xreds/virtual-icechunk-s3-http.patch`. The image
follows upstream's `Dockerfile.viewer`: the bundled React viewer is served at
`http://localhost:8090/`, the API at `/datasets/...` and its docs at `/docs`. It reads its
catalog from `./repos/_catalogs/xreds.yml`, mounted into the container
(Xreds can also read it from S3 or HTTP: any fsspec URL, e.g.
`s3://icechunk/_catalogs/xreds.yml`).

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

## Multiscale overviews

A web map that shows a whole model domain would otherwise fetch every
full-resolution chunk. With overviews, a repository also holds coarser copies
(2x, 4x, ... fewer cells per side) of selected variables, and a map loads the
level that matches the zoom. The store becomes a *hybrid*: full resolution stays
virtual (references into the provider's files), only the coarser levels are real
data. The layout follows the GeoZarr
[multiscales](https://github.com/zarr-conventions/multiscales),
[proj](https://github.com/zarr-conventions/proj) and
[spatial](https://github.com/zarr-conventions/spatial) conventions, with
metadata and chunking from [topozarr](https://github.com/carbonplan/topozarr):

```
wcofs-rg.icechunk/
  /                 the virtual dataset, unchanged (gridlook, Xreds, xarray)
  /multiscale/      multiscales + proj: + spatial: attributes
     0/             the overview variables again, as the same references
     1/ .. N-1/     materialized levels, each 2x2-averaged from the one before
```

```bash
./virtualize.py wcofs-regulargrid-nowcast --repo s3://icechunk/wcofs-rg.icechunk \
    --start 2026-11-01 --end 2026-11-07
```

The `*-regulargrid-nowcast` presets build overviews by default
(`--no-overviews` to skip). Each batch writes its references and its overview
steps in the same commit, so an interrupted build resumes cleanly and appends
keep every level in step with the root. Level k is computed in memory from
level k-1, so each source value is read once. `--force` rewrites the overview
steps of re-ingested files too. Overview settings are fixed when the repository
is created; a repository built without overviews can't gain them later (build a
new one).

**Only regular grids.** Each level is described by one affine transform, so the
grid must be a regular lat/lon grid. The ROMS `fields` presets are curvilinear
and can't have overviews; NOAA's `regulargrid` files carry the same runs
interpolated to a regular grid (WCOFS 0.04°, CIOFS 0.005°), at fixed depths,
with currents rotated to east/north. `rectilinear` turns their 2-D
`Latitude`/`Longitude` into 1-D `lat`/`lon`.

**Cost.** Overview levels are real data: per time step about a third of each
variable's full size. For WCOFS a 3-D variable is ~128 MB per step, so ~43 MB
of overviews before compression. Building them reads those variables in full
from NOAA, which dominates build time. Choose `overviews.variables` (or
`--overview-variables`) accordingly. For scale: one week of WCOFS regulargrid
(56 steps, the default five variables, 3 levels) took 1 h 53 min on a ~5 MB/s
link (~2 min per step) and produced a 2.7 GB repository (~48 MB per step after
compression).

**Reading.** `xr.open_datatree(store, group="multiscale", engine="zarr",
consolidated=False)` gives one child per level. In the browser, use the maps
viewer below. Of the tools in this setup, only it uses the levels: gridlook and
Xreds keep showing the full-resolution root.

### Viewing overviews on a map

`docker compose up -d` also builds and starts a small MapLibre page
(`./viewer`) on `http://localhost:8080`, using
[zarr-layer](https://github.com/carbonplan/zarr-layer) (multiscales-aware) and
[icechunk-js](https://github.com/EarthyScience/icechunk-js). It lists the
repositories that the gridlook catalog tags `overviews`, or opens one directly
with `http://localhost:8080/?repo=http://localhost:7070/icechunk/<name>.icechunk`.
Coarse levels come from the local S3 server; full resolution is read through
the virtual references straight from NOAA's bucket (which allows browser
reads). Controls: repository, variable, level (Auto picks it by zoom; choosing
one locks the map to it at every zoom, also `?level=N`), displayed time (the main slider; hovering it or the loop band shows the date there), an
animation loop (start and end times, default the whole series, also
`?from=N&to=N`; a band under the slider shows the window; drag on it to select a new one), Play (steps
through the loop and wraps from end back to start, waiting for each step to
finish loading and then a chosen delay, 2 s to 0.1 s, default 0.1 s), depth, colour range (defaults to
the 2nd–98th percentile of the coarsest level; **Fit to view** sets it to the
min and max of the data in view, at the level drawn); click the map for a value. The
basemap is OpenStreetMap's tile server, so it needs internet access.

Build and run the viewer independently (no local S3 service required when using
public remote repositories):

```bash
docker build -t icechunk-viewer ./viewer
docker run --rm -p 8080:80 \
    -e CATALOG_URL=https://example-bucket.s3.us-west-2.amazonaws.com/_catalogs/gridlook.json \
    icechunk-viewer
```

`CATALOG_URL` also accepts `s3://example-bucket/_catalogs/gridlook.json`,
which the browser translates to `https://example-bucket.s3.amazonaws.com/_catalogs/gridlook.json`.
Use a public regional HTTPS endpoint if your bucket requires one. For Compose:

```bash
CATALOG_URL=s3://example-bucket/_catalogs/gridlook.json docker compose up -d --build viewer
```

The entrypoint writes `CATALOG_URL` to `config.js` at container startup.
Change the environment variable and recreate the container to use another catalog;
the same image can be reused.
Without a value, the viewer uses the page's hostname on port 7070 at
`/icechunk/_catalogs/gridlook.json`, as in the local Compose setup.
`?catalog=<URL>` overrides either default, including with an `s3://` URL.
The catalog is fetched on each page load, rather than copied into the image.
It must use the gridlook catalog format with public browser-accessible repository
URLs and entries tagged `overviews`. Remote catalogs and repositories need public
read access and CORS allowing the viewer's origin; repository reads also need
range requests (see [Local S3](#local-s3-versitygw)). No AWS credentials are used.
For a build outside Docker, run `npm run build` in `viewer/` and set
`window.CATALOG_URL` in `dist/config.js` before serving it.

The map starts zoomed out to the coarsest overview level, and the status line
names the level the current zoom reads. zarr-layer moves to a finer level as
soon as the coarser one would be stretched (for WCOFS: L2 up to zoom ≈ 3.1, L1
to ≈ 4.1, then full resolution from NOAA). The **Data loads** panel (bottom left, collapsed at first; ▸ expands it)
lists every chunk read with its level, variable and chunk index, and totals per
source: overview reads from the repository in teal, full-resolution reads from
NOAA in amber with the source file and byte offset (hover for the full URL and
range).

Other clients that read GeoZarr multiscales are OpenLayers' `GeoZarr` source and
GDAL ≥ 3.13 (QGIS), but those read plain Zarr, not Icechunk.

## Notes

- The local-filesystem repository is not safe for concurrent writers: run one
  build at a time per repository.
- The `s3://` destination has been tested against the local versitygw
  container (create, append, re-run). It has not been tested against AWS itself.
- Opening source files runs at roughly one file per second overall; a month of
  WCOFS nowcast fields (248 files) takes about 4 minutes.
