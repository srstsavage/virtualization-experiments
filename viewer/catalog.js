// Catalogs stay remote: the browser reads the latest version on each page load.
export function catalogUrl(override, configured, location) {
  const url = override ?? (configured || `${location.protocol}//${location.hostname}:7070/icechunk/_catalogs/gridlook.json`)
  if (!/^s3:\/\//i.test(url)) return url
  // Preserve the key's URL encoding, query string and fragment verbatim.
  const match = /^s3:\/\/([^/?#]+)(\/[^?#]+)([?#].*)?$/i.exec(url)
  if (!match) throw new Error(`invalid S3 catalog URL: ${url}`)
  return `https://${match[1]}.s3.amazonaws.com${match[2]}${match[3] ?? ''}`
}
