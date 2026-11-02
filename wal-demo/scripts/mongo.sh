#!/usr/bin/env bash
set -euo pipefail
docker_local() {
  env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH \
    docker --host=unix:///var/run/docker.sock "$@"
}
image=mongo@sha256:b9924c330a8bbc5800fe9c69f04a84e221eca7b28fd049a0354147d9908fa142
if docker_local container inspect game-assets-mongo >/dev/null 2>&1; then
  actual=$(docker_local inspect --format '{{.Config.Image}}' game-assets-mongo)
  if [[ "$actual" != "$image" ]]; then
    echo 'Container game-assets-mongo already exists with another image; refusing to replace it.' >&2
    exit 1
  fi
  docker_local start game-assets-mongo >/dev/null
else
  docker_local run -d --name game-assets-mongo \
    -p 127.0.0.1:27017:27017 -v game-assets-mongo-data:/data/db \
    "$image" --bind_ip_all >/dev/null
fi
for ((i=0; i<30; i++)); do
  if docker_local exec game-assets-mongo mongosh --quiet --eval 'quit(db.adminCommand({ping: 1}).ok === 1 ? 0 : 1)' >/dev/null 2>&1; then
    echo 'MongoDB ready on 127.0.0.1:27017'
    exit 0
  fi
  sleep 1
done
echo 'MongoDB did not become ready; inspect docker logs game-assets-mongo.' >&2
exit 1
