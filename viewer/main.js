// Web map of the multiscale overviews that virtualize.py writes into a virtual
// Icechunk repository (group "multiscale", GeoZarr multiscales). zarr-layer picks
// the pyramid level for the zoom: coarse levels come from the repository, level 0
// is read through its virtual references straight from the source bucket.
//
// The view starts zoomed out far enough to read the coarsest level. A "Data loads"
// widget lists every chunk read, overview levels and source-bucket reads apart.
//
// Query parameters: catalog=<gridlook catalog URL> (lists repositories tagged
// "overviews"), repo=<repository URL> (open one directly), variable=, time=, depth=
// (indices), from=&to= (animation loop bounds, time indices), level= (lock a pyramid level; default auto), zoom=&lat=&lon= (initial
// view), debug (log chunk keys in window.__keys).
import * as maplibregl from 'maplibre-gl'
import { ZarrLayer } from '@carbonplan/zarr-layer'
import { IcechunkStore } from 'icechunk-js'
import * as zarr from 'zarrita'
import { createLoadTracker, mountLoadsWidget } from './loads.js'

const GROUP = 'multiscale'
const q = new URLSearchParams(location.search)
const CATALOG = q.get('catalog') ?? `${location.protocol}//${location.hostname}:7070/icechunk/_catalogs/gridlook.json`
const SEQUENTIAL = ['#440154', '#46327e', '#365c8d', '#277f8e', '#1fa187', '#4ac16d', '#a0da39', '#fde725']
const DIVERGING = ['#2166ac', '#4393c3', '#92c5de', '#d1e5f0', '#fddbc7', '#f4a582', '#d6604d', '#b2182b']

const $ = (id) => document.getElementById(id)
const ui = { repo: $('repo'), variable: $('variable'), time: $('time'), depth: $('depth'), cmin: $('cmin'), cmax: $('cmax'),
  play: $('play'), delay: $('delay'), fitview: $('fitview'), level: $('level'), tstart: $('tstart'), tend: $('tend') }
const state = { store: null, ms: null, layer: null, layerLoading: false, vars: {}, times: [], depths: null, levels: 0, maxZooms: [] }

if (q.has('debug')) window.__keys = []
const tracker = createLoadTracker({
  dims: (path) => state.store?.getMetadata(path)?.dimension_names ?? null,
  debugKeys: q.has('debug') ? window.__keys : null,
})
mountLoadsWidget($('loads'), tracker)

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
  zoom: 2,
})
map.addControl(new maplibregl.NavigationControl(), 'top-right')

// --- reading the repository ------------------------------------------------
const meta = (path) => state.store.getMetadata(path)
const children = (path) => state.store.listChildren(path).map((c) => String(c).split('/').pop())

async function readArray(path, selection = null) {
  const arr = await zarr.open(tracker.wrap(state.store.resolve(path), path), { kind: 'array' })
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
  setPlaying(false)
  tracker.clear()
  // fetchClient sees the requests for virtual chunks, i.e. reads from the source bucket.
  state.store = await IcechunkStore.open(url.replace(/\/$/, ''), { fetchClient: tracker.fetchClient })
  const root = meta(`/${GROUP}`)
  if (!root) throw new Error(`${url} has no ${GROUP} group (built without overviews?)`)
  const attrs = root.attributes ?? {}
  state.levels = attrs.multiscales?.layout?.length ?? 0
  state.maxZooms = levelMaxZooms(attrs)
  state.ms = tracker.wrap(state.store.resolve(GROUP), `/${GROUP}`)
  const shapes = (attrs.multiscales?.layout ?? []).map((l) => l['spatial:shape'])
  const wanted = q.get('level') ?? ui.level.value
  ui.level.innerHTML = '<option value="auto">Auto (by zoom)</option>' + shapes.map((sh, i) =>
    `<option value="${i}">${i === 0 ? 'L0 full resolution (NOAA)' : `L${i} overview`}${sh ? ` · ${sh[1]}×${sh[0]}` : ''}</option>`).join('')
  ui.level.value = wanted !== 'auto' && Number(wanted) < shapes.length ? String(Number(wanted)) : 'auto'
  q.delete('level')

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
  const opts = state.times.map((t, i) => `<option value="${i}">${t}</option>`).join('')
  ui.tstart.innerHTML = opts; ui.tend.innerHTML = opts
  const last = state.times.length - 1
  const clampT = (v, d) => (v !== null && /^\d+$/.test(v) ? String(Math.min(last, Number(v))) : String(d))
  ui.tstart.value = clampT(q.get('from'), 0)
  ui.tend.value = clampT(q.get('to'), last)
  if (Number(ui.tstart.value) > Number(ui.tend.value)) ui.tstart.value = ui.tend.value
  showTime()

  state.depths = meta(`/${GROUP}/0/Depth`) ? await readArray(`/${GROUP}/0/Depth`) : null
  ui.depth.innerHTML = (state.depths ?? []).map((d, i) => `<option value="${i}">${d} m</option>`).join('')
  ui.depth.value = q.get('depth') ?? '0'

  const bbox = attrs['spatial:bbox']
  if (q.has('zoom') && q.has('lat') && q.has('lon')) { // a given view wins, once
    map.jumpTo({ zoom: Number(q.get('zoom')), center: [Number(q.get('lon')), Number(q.get('lat'))] })
    q.delete('zoom')
  } else if (bbox) { // whole dataset in view, zoomed out to the coarsest level
    const cam = map.cameraForBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40 })
    const coarsest = state.maxZooms[state.levels - 1]
    let zoom = (cam?.zoom ?? 3) - 1
    if (Number.isFinite(coarsest)) zoom = Math.min(zoom, coarsest - 0.05)
    map.jumpTo({ center: cam?.center ?? [(bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2], zoom: Math.max(0, zoom) })
  }
  await showVariable()
}

// --- levels --------------------------------------------------------------------
// The highest map zoom each level serves, by zarr-layer's rule (selectLevelForZoom):
// a level is used while its longitude pixels per world width >= 256 * 2^zoom.
function levelMaxZooms(attrs) {
  return (attrs.multiscales?.layout ?? []).map((l) => {
    const shape = l['spatial:shape'], tf = l['spatial:transform']
    if (!shape || !tf) return NaN
    const span = shape[1] * Math.abs(tf[0])
    return Math.log2(shape[1] / (span / 360) / 256)
  })
}

// The level zarr-layer reads at this zoom: the coarsest one that still serves it.
function levelForZoom(zoom) {
  for (let i = state.maxZooms.length - 1; i >= 0; i--) if (state.maxZooms[i] >= zoom) return i
  return 0
}

function lockedLevel() {
  return ui.level.value === 'auto' ? null : Number(ui.level.value)
}

function levelStatus() {
  const zoom = map.getZoom()
  const locked = lockedLevel()
  const lvl = locked ?? levelForZoom(zoom)
  const what = lvl > 0 ? `overview L${lvl} of ${state.levels - 1}` : 'L0 full resolution (NOAA)'
  return `${locked === null ? '' : 'locked '}${what} · zoom ${zoom.toFixed(1)}`
}

// zarr-layer takes its levels from the group's multiscales.layout. To lock the map to
// one level, serve that metadata with only the chosen level's entry: zarr-layer then
// sees a one-level pyramid and reads that level at every zoom.
function lockLevel(store, level) {
  if (level === null) return store
  return {
    async get(key, opts) {
      const data = await store.get(key, opts)
      if (!data || key.replace(/^\//, '') !== 'zarr.json') return data
      const m = JSON.parse(new TextDecoder().decode(data))
      const ms = m.attributes?.multiscales
      if (ms?.layout) ms.layout = ms.layout.filter((l) => l.asset === String(level))
      return new TextEncoder().encode(JSON.stringify(m))
    },
    getRange: (key, range, opts) => store.getRange(key, range, opts),
  }
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

async function showVariable({ keepClim = false } = {}) {
  const name = ui.variable.value
  $('depthbox').style.display = state.vars[name].dimension_names.includes('Depth') ? '' : 'none'
  showTime()
  let clim = [Number(ui.cmin.value), Number(ui.cmax.value)]
  if (!keepClim || !(clim[0] < clim[1])) {
    clim = await autoRange()
    ui.cmin.value = fmt(clim[0]); ui.cmax.value = fmt(clim[1])
  }
  const colormap = updateLegend(clim)
  if (state.layer) map.removeLayer(state.layer.id)
  state.layer = new ZarrLayer({
    id: `ofs-${Date.now()}`,
    store: lockLevel(state.ms, lockedLevel()),
    variable: name,
    selector: selector(),
    colormap,
    clim,
    opacity: 0.9,
    onLoadingStateChange: (s) => {
      state.layerLoading = !!s.loading
      if (s.error) status(String(s.error), true)
      else status(levelStatus())
    },
  })
  state.layerLoading = true // until the new layer reports otherwise
  map.addLayer(state.layer)
}

// --- controls ----------------------------------------------------------------
// The displayed time, and the loop window drawn as a band under the time slider.
function showTime() {
  $('timelabel').textContent = state.times[Number(ui.time.value)] ?? ''
  const last = state.times.length - 1
  const band = $('loopband').firstElementChild
  const a = last > 0 ? Number(ui.tstart.value) / last : 0, b = last > 0 ? Number(ui.tend.value) / last : 1
  band.style.left = `${a * 100}%`
  band.style.width = `max(4px, ${(b - a) * 100}%)`
  $('loopband').setAttribute('aria-label', `Animation loop: ${state.times[ui.tstart.value] ?? ''} – ${state.times[ui.tend.value] ?? ''}. Drag to select a new loop.`)
}

// Time steps along the slider and the loop band. Both span the same positions: the
// band is inset by half the slider's thumb (THUMB/2 each side), the thumb's travel.
const THUMB = 16
function barIndex(el, x) {
  const r = el.getBoundingClientRect(), inset = el === ui.time ? THUMB / 2 : 0, last = state.times.length - 1
  const f = (x - r.left - inset) / (r.width - 2 * inset)
  return Math.max(0, Math.min(last, Math.round(f * last)))
}
// The date under the pointer, in a tip above the bar, pointing at the step's position.
function showTip(el, i) {
  const tip = $('timetip'), box = $('timebars').getBoundingClientRect(), r = el.getBoundingClientRect()
  const inset = el === ui.time ? THUMB / 2 : 0, last = state.times.length - 1
  tip.textContent = state.times[i] ?? ''
  tip.hidden = false
  const x = r.left - box.left + inset + (last > 0 ? i / last : 0) * (r.width - 2 * inset)
  const half = tip.offsetWidth / 2
  tip.style.left = `${Math.max(half, Math.min(box.width - half, x))}px`
  tip.style.top = `${r.top - box.top}px`
}
const hideTip = () => { $('timetip').hidden = true }
const inside = (el, e) => { const r = el.getBoundingClientRect(); return e.clientX >= r.left && e.clientX <= r.right && e.clientY >= r.top && e.clientY <= r.bottom }

ui.time.addEventListener('pointermove', (e) => { if (state.times.length) showTip(ui.time, barIndex(ui.time, e.clientX)) })
ui.time.addEventListener('pointerleave', hideTip)

const loopWindow = () => [Number(ui.tstart.value), Number(ui.tend.value)]

let timer = null
ui.time.addEventListener('input', () => {
  showTime()
  clearTimeout(timer)
  timer = setTimeout(() => state.layer?.setSelector(selector()), 150)
})

// Play: step through the times in a loop. Each step waits until the current slice
// has finished loading (layer idle, no chunk reads in flight), then the chosen delay.
const anim = { token: 0 }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const nextFrame = () => new Promise((r) => requestAnimationFrame(() => r()))

async function sliceLoaded(token) {
  // The layer starts the fetches for a new selector within two or three frames.
  // Once it has been seen loading, two quiet frames mean done; a slice that never
  // loads (already cached) is done after six quiet frames (~100 ms).
  let quiet = 0, seenBusy = false
  while (anim.token === token) {
    await nextFrame()
    const busy = state.layerLoading || tracker.inflight > 0
    seenBusy ||= busy
    quiet = busy ? 0 : quiet + 1
    if (quiet >= (seenBusy ? 2 : 6)) return true
  }
  return false
}

async function playLoop(token) {
  while (anim.token === token) {
    if (!(await sliceLoaded(token))) return
    // Re-read the delay while waiting, so a change applies to the current step.
    const t0 = performance.now()
    let left
    while (anim.token === token && (left = Number(ui.delay.value) - (performance.now() - t0)) > 0) await sleep(Math.min(left, 50))
    if (anim.token !== token || !state.times.length) return
    const [a, b] = loopWindow()
    const cur = Number(ui.time.value)
    setTime(cur < a || cur >= b ? a : cur + 1) // wraps from the end back to the start
  }
}

function setTime(i) {
  if (Number(ui.time.value) === i) return
  ui.time.value = String(i)
  showTime()
  clearTimeout(timer)
  state.layer?.setSelector(selector())
}

function setPlaying(on) {
  const token = ++anim.token
  ui.play.setAttribute('aria-pressed', String(on))
  ui.play.textContent = on ? '❚❚ Pause' : '▶ Play'
  if (on) {
    const [a, b] = loopWindow()
    const cur = Number(ui.time.value)
    if (cur < a || cur > b) setTime(a) // play within the loop window
  }
  if (on) playLoop(token).catch((e) => { setPlaying(false); status(String(e), true) })
}
ui.play.addEventListener('click', () => setPlaying(ui.play.getAttribute('aria-pressed') !== 'true'))
// Drag on the loop band to select a new window; a click without dragging moves the
// nearer bound to the clicked time.
{
  // The tip shows the time under the pointer: while dragging, the end being moved.
  const band = $('loopband')
  const indexAt = (x) => barIndex(band, x)
  let anchor = null, moved = false
  band.addEventListener('pointerdown', (e) => {
    if (state.times.length < 2 || e.button !== 0) return
    anchor = indexAt(e.clientX); moved = false
    band.setPointerCapture(e.pointerId)
    e.preventDefault()
  })
  band.addEventListener('pointermove', (e) => {
    if (!state.times.length) return
    const i = indexAt(e.clientX)
    showTip(band, i)
    if (anchor === null) return
    if (i === anchor && !moved) return
    moved = true
    ui.tstart.value = String(Math.min(anchor, i)); ui.tend.value = String(Math.max(anchor, i))
    showTime()
  })
  const end = (e) => {
    if (anchor === null) return
    if (!moved && e.type === 'pointerup') {
      const [a, b] = loopWindow()
      if (Math.abs(anchor - a) <= Math.abs(anchor - b)) ui.tstart.value = String(Math.min(anchor, b))
      else ui.tend.value = String(Math.max(anchor, a))
      showTime()
    }
    anchor = null
    if (!inside(band, e)) hideTip() // released outside: pointerleave came while captured
  }
  band.addEventListener('pointerleave', () => { if (anchor === null) hideTip() })
  band.addEventListener('pointerup', end)
  band.addEventListener('pointercancel', end)
}

// Keep start <= end by moving the other bound; a playing loop picks up the new window on its next step.
ui.tstart.addEventListener('change', () => {
  if (Number(ui.tstart.value) > Number(ui.tend.value)) ui.tend.value = ui.tstart.value
  showTime()
})
ui.tend.addEventListener('change', () => {
  if (Number(ui.tend.value) < Number(ui.tstart.value)) ui.tstart.value = ui.tend.value
  showTime()
})

ui.depth.addEventListener('change', () => state.layer?.setSelector(selector()))
ui.level.addEventListener('change', () => showVariable({ keepClim: true }).catch((e) => status(String(e), true)))
ui.variable.addEventListener('change', () => showVariable().catch((e) => status(String(e), true)))
ui.repo.addEventListener('change', () => openRepo(ui.repo.value).catch((e) => status(String(e), true)))
function applyClim() {
  const clim = [Number(ui.cmin.value), Number(ui.cmax.value)]
  if (!(clim[0] < clim[1]) || !state.layer) return
  state.layer.setClim(clim)
  state.layer.setColormap(updateLegend(clim))
}
for (const input of [ui.cmin, ui.cmax]) input.addEventListener('change', applyClim)

// Fit to view: min and max of the current slice inside the visible map, read at the
// level being drawn (so mostly from chunks already loaded). The query polygon is the
// viewport's corners, which follows rotation; cells with no data are left out.
async function fitToView() {
  if (!state.layer) return
  const { width, height } = map.getContainer().getBoundingClientRect()
  const ring = [[0, 0], [width, 0], [width, height], [0, height]].map(([x, y]) => {
    const p = map.unproject([x, y])
    return [Math.max(-720, Math.min(720, p.lng)), Math.max(-85, Math.min(85, p.lat))]
  })
  ring.push(ring[0])
  const r = await state.layer.queryData({ type: 'Polygon', coordinates: [ring] }, undefined,
    { includeSpatialCoordinates: false })
  let lo = Infinity, hi = -Infinity
  for (const v of r?.[ui.variable.value] ?? []) {
    if (!Number.isFinite(v) || Math.abs(v) >= 1e30) continue
    if (v < lo) lo = v
    if (v > hi) hi = v
  }
  if (!(lo < hi)) { status(lo === hi ? `every cell in view is ${fmt(lo)}` : 'no data in view', true); return }
  const same = fmt(lo) === fmt(hi) // a narrow range that rounding would collapse
  ui.cmin.value = same ? String(lo) : fmt(lo); ui.cmax.value = same ? String(hi) : fmt(hi)
  applyClim()
  status(levelStatus())
}
ui.fitview.addEventListener('click', async () => {
  ui.fitview.disabled = true
  try { await fitToView() } catch (e) { status(String(e.message ?? e), true) } finally { ui.fitview.disabled = false }
})
map.on('zoomend', () => { if (state.layer && !$('status').classList.contains('error')) status(levelStatus()) })
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
window.__viewer = { state, map, tracker }
