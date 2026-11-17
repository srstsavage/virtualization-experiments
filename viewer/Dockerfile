FROM dhi.io/node:26-alpine3.23-dev AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html config.js main.js loads.js catalog.js build.mjs ./
RUN npm run build

FROM nginx:1.31
ENV CATALOG_URL=s3://grid-virtualization/_catalogs/gridlook.json
COPY --chmod=755 docker-entrypoint.sh /docker-entrypoint.d/40-viewer-config.sh
COPY --from=build /app/dist /usr/share/nginx/html
