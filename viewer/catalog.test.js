import test from 'node:test'
import assert from 'node:assert/strict'
import { catalogUrl } from './catalog.js'

const location = { protocol: 'http:', hostname: 'viewer.example' }

test('catalog selection preserves the local default and query override', () => {
  assert.equal(catalogUrl(null, '', location), 'http://viewer.example:7070/icechunk/_catalogs/gridlook.json')
  assert.equal(catalogUrl(null, 'https://example.org/catalog.json', location), 'https://example.org/catalog.json')
  assert.equal(catalogUrl('/override.json', 'https://example.org/catalog.json', location), '/override.json')
})

test('HTTP(S) and relative URLs pass through unchanged', () => {
  for (const url of ['http://localhost:7070/catalog.json', 'https://bucket.s3.us-west-2.amazonaws.com/catalog.json', './catalog.json']) {
    assert.equal(catalogUrl(null, url, location), url)
  }
})

test('S3 URLs use anonymous HTTPS and preserve encoded keys', () => {
  const url = 's3://example-bucket/a%20folder/catalog.json?x=a%26b#fragment'
  const expected = 'https://example-bucket.s3.amazonaws.com/a%20folder/catalog.json?x=a%26b#fragment'
  assert.equal(catalogUrl(null, url, location), expected)
  assert.equal(catalogUrl(url, '/ignored.json', location), expected)
  assert.throws(() => catalogUrl(null, 's3://example-bucket', location), /invalid S3 catalog URL/)
})
