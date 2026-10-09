# Built from the checkout it sits in: the build context is the repository root, so
# this builds whatever is checked out - sead-deployment checks out the release's tag.
#   podman build -f docker/Dockerfile .
# In dev mode the checkout is mounted over the copy of the source made here, and
# nodemon restarts the server on changes (see compose.override.yml in sead-deployment).
FROM docker.io/debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates curl gnupg
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=22
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update && apt-get install -y nodejs nano tini

# Install nodemon for development mode
RUN npm install -g nodemon

ENV NODE_OPTIONS="--max-old-space-size=4096"

WORKDIR /json_api_server

# The dependencies first, so they are only installed again when the lockfile changes
COPY package.json package-lock.json ./
RUN npm ci --legacy-peer-deps

COPY . .

# Build-time ARG
ARG MODE=prod
ARG DOMAIN=localhost

# Persist the mode for runtime
ENV MODE=${MODE}
ENV DOMAIN=${DOMAIN}

# What was built, as 'git describe' put it; reported by 'deploy.sh versions'
ARG SOURCE_VERSION=
ENV SOURCE_VERSION=${SOURCE_VERSION}

# tini is PID 1 so that exited processes get reaped. When a healthcheck times out, podman
# gives up on it and its curl is left as a child of PID 1. With node as PID 1 (the exec
# below) nothing reaped them, and the zombies piled up until the container hit its pids
# limit. tini passes SIGTERM on to node, so the graceful shutdown still works.
ENTRYPOINT ["/usr/bin/tini", "--"]

# The server is asked over loopback, not through ${DOMAIN}: on a host running several
# deployments, port 8484 is not published on the public address, and the connection
# hung until the healthcheck timed out. --max-time ends curl before podman's timeout does.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -fsS --max-time 8 http://127.0.0.1:8484/health || exit 1

# Use conditional CMD based on MODE
CMD ["sh", "-c", "if [ \"$MODE\" = \"dev\" ]; then nodemon --legacy-watch /json_api_server/src/index.js; else exec node /json_api_server/src/index.js; fi"]
