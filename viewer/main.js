// Web map of the multiscale overviews that virtualize.py writes into a virtual
// Icechunk repository (group "multiscale", GeoZarr multiscales). zarr-layer picks
// the pyramid level for the zoom: coarse levels come from the repository, level 0
// is read through its virtual references straight from the source bucket.
//
// Query parameters: catalog=<gridlook catalog URL> (lists repositories tagged
// "overviews"), repo=<repository URL> (open one directly), variable=, time=, depth=
// (indices), zoom=&lat=&lon= (initial view), debug (log store keys in window.__keys).
import * as maplibregl from 'maplibre-gl'
import { ZarrLayer } from '@carbonplan/zarr-layer'
import { IcechunkStore } from 'icechunk-js'
import * as zarr from 'zarrita'

const GROUP = 'multiscale'
const q = new URLSearchParams(location.search)
const CATALOG = q.get('catalog') ?? `${location.protocol}//${location.hostname}:7070/icechunk/_catalogs/gridlook.json`
const SEQUENTIAL = ['#440154', '#46327e', '#365c8d', '#277f8e', '#1fa187', '#4ac16d', '#a0da39', '#fde725']
const DIVERGING = ['#2166ac', '#4393c3', '#92c5de', '#d1e5f0', '#fddbc7', '#f4a582', '#d6604d', '#b2182b']

const $ = (id) => document.getElementById(id)
const ui = { repo: $('repo'), variable: $('variable'), time: $('time'), depth: $('depth'), cmin: $('cmin'), cmax: $('cmax') }
const state = { store: null, ms: null, layer: null, vars: {}, times: [], depths: null, levels: 0 }

function status(text, error = false) {
  $('status').textContent = text
  $('status').classList.toggle('error', error)
}

const map = new maplibregl.Map({
  container: 'map',
  style: {
    version: 8,
    sources: {
      osm: { type: 'raster', tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'], tileSize: 256,
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors' },
    },
    layers: [{ id: 'osm', type: 'raster', source: 'osm', paint: { 'raster-saturation': -0.6, 'raster-opacity': 0.8 } }],
  },
  center: [-125, 40],
  zoom: 3,
})
map.addControl(new maplibregl.NavigationControl(), 'top-right')

// --- reading the repository ------------------------------------------------
const meta = (path) => state.store.getMetadata(path)
const children = (path) => state.store.listChildren(path).map((c) => String(c).split('/').pop())

async function readArray(path, selection = null) {
  const arr = await zarr.open(state.store.resolve(path), { kind: 'array' })
  const chunk = await zarr.get(arr, selection)
  return Array.from(chunk.data, Number)
}

function decodeTimes(values, units) {
  const m = /^\s*(\w+)\s+since\s+(.+?)\s*$/.exec(units ?? '')
  if (!m) return values.map(String)
  const scale = { seconds: 1e3, second: 1e3, minutes: 6e4, minute: 6e4, hours: 3.6e6, hour: 3.6e6, days: 8.64e7, day: 8.64e7 }[m[1]]
  let ref = m[2].trim().replace(' ', 'T')
  if (!/[zZ]|[+-]\d\d:?\d\d$/.test(ref)) ref += 'Z'
  const t0 = Date.parse(ref)
  return values.map((v) => new Date(t0 + v * scale).toISOString().slice(0, 16).replace('T', ' ') + ' UTC')
}

async function openRepo(url) {
  status('Opening repository…')
  if (state.layer) { map.removeLayer(state.layer.id); state.layer = null }
  state.store = await IcechunkStore.open(url.replace(/\/$/, ''))
  const root = meta(`/${GROUP}`)
  if (!root) throw new Error(`${url} has no ${GROUP} group (built without overviews?)`)
  const attrs = root.attributes ?? {}
  state.levels = attrs.multiscales?.layout?.length ?? 0
  state.ms = state.store.resolve(GROUP)
  if (q.has('debug')) { // record the keys zarr-layer reads (window.__keys), e.g. to see which level loads
    const ms = state.ms
    window.__keys = []
    state.ms = {
      get: (key, opts) => { window.__keys.push(key); return ms.get(key, opts) },
      getRange: (key, range, opts) => { window.__keys.push(key); return ms.getRange(key, range, opts) },
    }
  }

  // Variables of level 0 that are over (time, ..., lat, lon).
  state.vars = {}
  for (const name of children(`/${GROUP}/0`)) {
    const m = meta(`/${GROUP}/0/${name}`)
    const dims = m?.dimension_names ?? []
    if (m?.node_type === 'array' && dims.includes('lat') && dims.includes('lon') && dims.length >= 3) {
      state.vars[name] = m
    }
  }
  ui.variable.innerHTML = Object.entries(state.vars)
    .map(([n, m]) => `<option value="${n}">${m.attributes?.long_name ? `${n}: ${m.attributes.long_name}` : n}</option>`).join('')
  if (q.get('variable') && state.vars[q.get('variable')]) ui.variable.value = q.get('variable')

  const timeName = state.vars[ui.variable.value].dimension_names[0]
  const tmeta = meta(`/${GROUP}/0/${timeName}`)
  state.times = decodeTimes(await readArray(`/${GROUP}/0/${timeName}`), tmeta?.attributes?.units)
  ui.time.max = String(state.times.length - 1)
  ui.time.value = q.get('time') ?? String(state.times.length - 1)

  state.depths = meta(`/${GROUP}/0/Depth`) ? await readArray(`/${GROUP}/0/Depth`) : null
  ui.depth.innerHTML = (state.depths ?? []).map((d, i) => `<option value="${i}">${d} m</option>`).join('')
  ui.depth.value = q.get('depth') ?? '0'

  const bbox = attrs['spatial:bbox']
  if (q.has('zoom') && q.has('lat') && q.has('lon')) { // a given view wins, once
    map.jumpTo({ zoom: Number(q.get('zoom')), center: [Number(q.get('lon')), Number(q.get('lat'))] })
    q.delete('zoom')
  } else if (bbox) {
    map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40, animate: false })
  }
  await showVariable()
}

// --- the layer ---------------------------------------------------------------
function selector() {
  const dims = state.vars[ui.variable.value].dimension_names
  const sel = { [dims[0]]: { selected: Number(ui.time.value), type: 'index' } }
  if (dims.includes('Depth')) sel.Depth = { selected: Number(ui.depth.value), type: 'index' }
  return sel
}

// Colour range from the coarsest level: 2nd..98th percentile of the current slice.
async function autoRange() {
  const name = ui.variable.value
  const m = state.vars[name]
  const sel = m.dimension_names.map((d) => (d === m.dimension_names[0] ? Number(ui.time.value)
    : d === 'Depth' ? Number(ui.depth.value) : null))
  const values = await readArray(`/${GROUP}/${state.levels - 1}/${name}`, sel)
  const fill = m.fill_value
  const ok = values.filter((v) => Number.isFinite(v) && v !== fill && Math.abs(v) < 1e30).sort((a, b) => a - b)
  if (!ok.length) return [0, 1]
  const lo = ok[Math.floor(ok.length * 0.02)], hi = ok[Math.floor(ok.length * 0.98)]
  if (lo < 0 && hi > 0) { const r = Math.max(-lo, hi); return [-r, r] } // symmetric for signed fields
  return [lo, hi]
}

function fmt(v) { return Math.abs(v) >= 100 ? v.toFixed(0) : Number(v.toPrecision(3)).toString() }

function updateLegend(clim) {
  const cmap = clim[0] < 0 && clim[1] > 0 ? DIVERGING : SEQUENTIAL
  $('legend').style.background = `linear-gradient(to right, ${cmap.join(',')})`
  $('lmin').textContent = fmt(clim[0])
  $('lmax').textContent = fmt(clim[1])
  $('lunits').textContent = state.vars[ui.variable.value].attributes?.units ?? ''
  return cmap
}

async function showVariable() {
  const name = ui.variable.value
  $('depthbox').style.display = state.vars[name].dimension_names.includes('Depth') ? '' : 'none'
  $('timelabel').textContent = state.times[Number(ui.time.value)] ?? ''
  const clim = await autoRange()
  ui.cmin.value = fmt(clim[0]); ui.cmax.value = fmt(clim[1])
  const colormap = updateLegend(clim)
  if (state.layer) map.removeLayer(state.layer.id)
  state.layer = new ZarrLayer({
    id: `ofs-${Date.now()}`,
    store: state.ms,
    variable: name,
    selector: selector(),
    colormap,
    clim,
    opacity: 0.9,
    onLoadingStateChange: (s) => {
      if (s.error) status(String(s.error), true)
      else status(s.loading ? 'Loading data…' : `${state.levels} levels · zoom ${map.getZoom().toFixed(1)}`)
    },
  })
  map.addLayer(state.layer)
}

// --- controls ----------------------------------------------------------------
let timer = null
ui.time.addEventListener('input', () => {
  $('timelabel').textContent = state.times[Number(ui.time.value)] ?? ''
  clearTimeout(timer)
  timer = setTimeout(() => state.layer?.setSelector(selector()), 150)
})
ui.depth.addEventListener('change', () => state.layer?.setSelector(selector()))
ui.variable.addEventListener('change', () => showVariable().catch((e) => status(String(e), true)))
ui.repo.addEventListener('change', () => openRepo(ui.repo.value).catch((e) => status(String(e), true)))
for (const input of [ui.cmin, ui.cmax]) {
  input.addEventListener('change', () => {
    const clim = [Number(ui.cmin.value), Number(ui.cmax.value)]
    if (!(clim[0] < clim[1]) || !state.layer) return
    state.layer.setClim(clim)
    state.layer.setColormap(updateLegend(clim))
  })
}
map.on('zoomend', () => { if (state.layer && !$('status').classList.contains('error')) status(`${state.levels} levels · zoom ${map.getZoom().toFixed(1)}`) })
map.on('click', async (e) => {
  if (!state.layer) return
  try {
    const r = await state.layer.queryData({ type: 'Point', coordinates: [e.lngLat.lng, e.lngLat.lat] })
    const v = r?.[ui.variable.value]?.[0]
    const units = state.vars[ui.variable.value].attributes?.units ?? ''
    new maplibregl.Popup().setLngLat(e.lngLat)
      .setText(Number.isFinite(v) ? `${ui.variable.value} = ${fmt(v)} ${units}` : 'no data').addTo(map)
  } catch (err) { status(String(err), true) }
})

// --- start ---------------------------------------------------------------------
async function start() {
  const direct = q.get('repo')
  let entries = []
  try {
    const catalog = await (await fetch(CATALOG)).json()
    entries = (catalog.datasets ?? []).filter((d) => d.tags?.overviews)
  } catch (e) {
    if (!direct) throw new Error(`could not read the catalog ${CATALOG}: ${e}`)
  }
  if (direct && !entries.some((d) => d.url === direct)) entries.unshift({ title: direct, url: direct })
  if (!entries.length) throw new Error('no repository with overviews in the catalog (build one with --overviews)')
  ui.repo.innerHTML = entries.map((d) => `<option value="${d.url}">${d.title}</option>`).join('')
  ui.repo.value = direct ?? entries[0].url
  await openRepo(ui.repo.value)
}
map.on('load', () => start().catch((e) => { console.error(e); status(String(e.message ?? e), true) }))
window.__viewer = { state, map }
