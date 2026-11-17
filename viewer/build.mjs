import { build } from 'esbuild'
import { copyFile } from 'node:fs/promises'

await build({
  entryPoints: ['main.js'],
  bundle: true,
  format: 'esm',
  minify: true,
  sourcemap: true,
  outfile: 'dist/bundle.js',
})

await Promise.all([
  copyFile('index.html', 'dist/index.html'),
  copyFile('config.js', 'dist/config.js'),
  ...['maplibre-gl.css', 'maplibre-gl-worker.mjs', 'maplibre-gl-shared.mjs'].map(
    (name) => copyFile(`node_modules/maplibre-gl/dist/${name}`, `dist/${name}`),
  ),
])
