#!/bin/sh
set -eu

# Base64 keeps quotes, backslashes, newlines and UTF-8 safe in JavaScript.
catalog_base64=$(printf '%s' "${CATALOG_URL:-}" | base64 | tr -d '\n')
printf 'window.CATALOG_URL = new TextDecoder().decode(Uint8Array.from(atob("%s"), c => c.charCodeAt(0)));\n' \
    "$catalog_base64" > /usr/share/nginx/html/config.js
