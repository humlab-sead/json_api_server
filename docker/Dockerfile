FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates curl gnupg
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
ENV NODE_MAJOR=22
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update && apt-get install -y nodejs git nano

WORKDIR /

# Build-time ARG
ARG JAS_REF=main
ARG MODE=prod

# Persist the mode for runtime
ENV MODE=${MODE}

RUN git clone --recurse-submodules https://github.com/humlab-sead/json_api_server.git \
    && cd json_api_server \
    && git checkout ${JAS_REF} \
    && git submodule update --init --recursive

WORKDIR /json_api_server

RUN npm install

# Install nodemon for development mode
RUN npm install -g nodemon

# Use conditional CMD based on MODE
CMD ["sh", "-c", "if [ \"$MODE\" = \"dev\" ]; then nodemon --legacy-watch /json_api_server/src/index.js; else node /json_api_server/src/index.js; fi"]
