// Tracks the chunk reads behind the map and lists them in a small widget, so it is
// visible which data comes from the repository's overview levels and which comes
// from the source bucket through level 0's virtual references.
//
// Two hooks: `wrap(store)` sees every zarr chunk key read (level, variable, chunk),
// and `fetchClient`, handed to IcechunkStore.open, sees the HTTP requests icechunk-js
// makes for *virtual* chunks only. Each wrapped read gets its own AbortSignal, which
// icechunk-js passes through to fetchClient, so a request is matched to its read.

const MAX_ENTRIES = 300

export function createLoadTracker({ dims = () => null, debugKeys = null } = {}) {
  let entries = []
  let totals = emptyTotals()
  let seq = 0
  const bySignal = new WeakMap()
  const listeners = new Set()
  let scheduled = false

  function emptyTotals() {
    return { overview: { n: 0, bytes: 0 }, raw: { n: 0, bytes: 0 }, local: { n: 0, bytes: 0 } }
  }
  function changed() {
    if (scheduled) return
    scheduled = true
    requestAnimationFrame(() => { scheduled = false; for (const fn of listeners) fn() })
  }
  function add(entry) {
    entries.push(entry)
    if (entries.length > MAX_ENTRIES) entries = entries.slice(-MAX_ENTRIES)
    changed()
    return entry
  }
  function finish(entry, status, bytes = 0) {
    entry.status = status
    entry.ms = performance.now() - entry.t0
    entry.bytes = bytes
    // A read with no request through fetchClient was served by the repository itself.
    if (!entry.source) entry.source = entry.level !== null && entry.level > 0 ? 'overview' : 'local'
    if (status === 'done') {
      totals[entry.source].n += 1
      totals[entry.source].bytes += bytes
    }
    changed()
  }

  // "/multiscale/1/temp/c/55/0/1/2" -> level 1, variable temp, coords [55,0,1,2]
  function parse(path) {
    const m = /^(.*?)\/c\/([\d/]+)$/.exec(path)
    if (!m) return null
    const arrayPath = m[1]
    const parts = arrayPath.split('/').filter(Boolean)
    const li = parts.indexOf('multiscale')
    const level = li >= 0 && /^\d+$/.test(parts[li + 1] ?? '') ? Number(parts[li + 1]) : null
    return { arrayPath, level, variable: parts[parts.length - 1], coords: m[2].split('/').map(Number) }
  }

  async function tracked(path, range, call, opts) {
    const info = parse(path)
    if (!info) return call(opts) // metadata: not listed
    debugKeys?.push(path)
    const ctl = new AbortController()
    const outer = opts?.signal
    if (outer) {
      if (outer.aborted) ctl.abort(outer.reason)
      else outer.addEventListener('abort', () => ctl.abort(outer.reason), { once: true })
    }
    const entry = add({
      id: ++seq, ...info, dims: dims(info.arrayPath), range, status: 'loading',
      t0: performance.now(), source: null, requests: [],
    })
    bySignal.set(ctl.signal, entry)
    try {
      const data = await call({ ...opts, signal: ctl.signal })
      finish(entry, 'done', data?.byteLength ?? 0)
      return data
    } catch (err) {
      finish(entry, ctl.signal.aborted ? 'aborted' : 'error')
      entry.error = String(err?.message ?? err)
      throw err
    }
  }

  return {
    get entries() { return entries },
    get totals() { return totals },
    get inflight() { return entries.filter((e) => e.status === 'loading').length },
    subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn) },
    clear() { entries = entries.filter((e) => e.status === 'loading'); totals = emptyTotals(); changed() },

    // For IcechunkStore.open(url, { fetchClient }): virtual chunk requests only.
    fetchClient: {
      async fetch(url, init) {
        const range = new Headers(init?.headers).get('Range')
        let entry = init?.signal ? bySignal.get(init.signal) : undefined
        if (!entry) { // not from a wrapped read: list it on its own so source traffic is never missed
          entry = add({ id: ++seq, arrayPath: null, level: null, variable: null, coords: null, dims: null,
            range: null, status: 'loading', t0: performance.now(), source: 'raw', requests: [], orphan: true })
        }
        entry.source = 'raw'
        entry.requests.push({ url, range })
        changed()
        try {
          const response = await globalThis.fetch(url, init)
          if (entry.orphan) finish(entry, response.ok ? 'done' : 'error', Number(response.headers.get('Content-Length') ?? 0))
          return response
        } catch (err) {
          if (entry.orphan) finish(entry, init?.signal?.aborted ? 'aborted' : 'error')
          throw err
        }
      },
    },

    // A zarrita store that lists the chunk reads of `store`; `prefix` is the
    // store's path in the repository, used to name the reads.
    wrap(store, prefix = '') {
      const full = (key) => `${prefix}${key.startsWith('/') ? '' : '/'}${key}`
      return {
        get: (key, opts) => tracked(full(key), null, (o) => store.get(key, o), opts),
        getRange: (key, range, opts) => tracked(full(key), range, (o) => store.getRange(key, range, o), opts),
      }
    },
  }
}

// --- widget ------------------------------------------------------------------

const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c])

export function formatBytes(n) {
  if (!n) return '0 B'
  if (n < 1024) return `${n} B`
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(n < 10240 ? 1 : 0)} kB`
  return `${(n / 1024 ** 2).toFixed(n < 10 * 1024 ** 2 ? 1 : 0)} MB`
}

function chunkLabel(e) {
  if (!e.coords) return ''
  const names = e.dims ?? []
  const parts = e.coords.map((c, i) => {
    const d = names[i]
    if (d === 'lat' || d === 'lon') return null
    return d ? `${d === 'time' ? 't' : d === 'Depth' ? 'd' : d}${c}` : null
  }).filter(Boolean)
  const xy = names.length
    ? e.coords.filter((_, i) => names[i] === 'lat' || names[i] === 'lon')
    : e.coords
  return (xy.length ? [...parts, `[${xy.join(',')}]`] : parts).join(' · ')
}

function rangeLabel(e) {
  if (!e.range) return ''
  if ('suffixLength' in e.range) return 'shard index'
  return 'inner chunk'
}

function requestLabel(r) {
  const file = r.url.split('?')[0].split('/').pop()
  const m = /bytes=(\d+)-(\d+)/.exec(r.range ?? '')
  const span = m ? ` @ ${(Number(m[1]) / 1024 ** 2).toFixed(1)} MB` : '' // offset in the file; full range in the tooltip
  return { file, span }
}

// The source a read will turn out to have, for its label while it loads, so the
// label doesn't change when it completes: overview levels are in the repository;
// level 0 holds map arrays as references to the source bucket and small
// coordinate arrays in the repository.
function expectedSource(e) {
  if (e.level === null) return 'pending'
  if (e.level > 0) return 'overview'
  const dims = e.dims ?? []
  return dims.includes('lat') && dims.includes('lon') ? 'raw' : 'local'
}

function row(e) {
  const kind = e.source ?? expectedSource(e)
  const badge = kind === 'raw' ? `NOAA${e.level !== null ? ` L${e.level}` : ''}`
    : kind === 'overview' ? `Overview L${e.level}`
    : kind === 'local' ? 'Repo' : '…'
  const title = e.variable ? `${esc(e.variable)} <span class="muted">${esc(chunkLabel(e))}</span>` : '<span class="muted">unmatched request</span>'
  const sub = e.requests.map((r) => {
    const { file, span } = requestLabel(r)
    return `<div class="file" title="${esc(r.url)} ${esc(r.range ?? '')}"><span class="name">${esc(file)}</span><span class="muted span">${esc(span)}</span></div>`
  }).join('') || (e.range ? `<div class="file muted">${esc(rangeLabel(e))}</div>` : '')
  const status = e.status === 'loading' ? '<span class="dot" aria-label="loading"></span>'
    : e.status === 'error' ? `<span class="err" title="${esc(e.error ?? '')}">✕</span>`
    : e.status === 'aborted' ? '<span class="muted">cancelled</span>'
    : `${formatBytes(e.bytes)} · ${e.ms < 1000 ? `${Math.round(e.ms)} ms` : `${(e.ms / 1000).toFixed(1)} s`}`
  return `<li class="load src-${kind} st-${e.status}" data-id="${e.id}">
    <span class="badge">${badge}</span>
    <div class="what">${title}</div>
    <span class="meta">${status}</span>${sub}</li>`
}

export function mountLoadsWidget(root, tracker) {
  root.innerHTML = `
    <div class="loads-head">
      <button type="button" class="toggle" aria-expanded="true" title="Show or hide the list">▾</button>
      <strong>Data loads</strong>
      <span class="inflight"></span>
      <button type="button" class="clear">Clear</button>
    </div>
    <div class="totals">
      <span class="chip src-overview"><span class="sw"></span><span class="t-overview"></span></span>
      <span class="chip src-raw"><span class="sw"></span><span class="t-raw"></span></span>
    </div>
    <ul class="list"></ul>`
  const list = root.querySelector('.list')
  const toggle = root.querySelector('.toggle')
  let collapsed = true // starts collapsed; the head still shows what is loading and the totals
  const applyCollapsed = () => {
    root.classList.toggle('collapsed', collapsed)
    toggle.setAttribute('aria-expanded', String(!collapsed))
    toggle.textContent = collapsed ? '▸' : '▾'
  }
  applyCollapsed()
  toggle.addEventListener('click', () => {
    collapsed = !collapsed
    applyCollapsed()
  })
  root.querySelector('.clear').addEventListener('click', () => tracker.clear())

  const render = () => {
    const t = tracker.totals
    root.querySelector('.t-overview').textContent = `Overviews ${t.overview.n} · ${formatBytes(t.overview.bytes)}`
    root.querySelector('.t-raw').textContent = `NOAA raw ${t.raw.n} · ${formatBytes(t.raw.bytes)}`
    const n = tracker.inflight
    root.querySelector('.inflight').innerHTML = n ? `<span class="dot"></span>${n} loading` : ''
    list.innerHTML = tracker.entries.slice().reverse().map(row).join('') || '<li class="empty muted">No chunk reads yet</li>'
  }
  tracker.subscribe(render)
  render()
}
