FROM node:24-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY runner.js ./
COPY scripts/apply-show-metadata-patch.mjs ./scripts/apply-show-metadata-patch.mjs
RUN node ./scripts/apply-show-metadata-patch.mjs && node --check ./runner.js && rm -rf ./scripts
COPY docs/ ./docs/

RUN mkdir -p /app/logs /app/cache

ENV LOG_DIR=/app/logs
ENV LOCAL_CACHE_DIR=/app/cache
ENV HEALTH_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD wget -qO- http://localhost:8080/status > /dev/null || exit 1

CMD ["node", "runner.js"]
