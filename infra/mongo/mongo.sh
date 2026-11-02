#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
compose_file="$script_dir/compose.yaml"

docker_local() {
  env -u DOCKER_HOST -u DOCKER_CONTEXT -u DOCKER_TLS -u DOCKER_TLS_VERIFY -u DOCKER_CERT_PATH \
    docker --host=unix:///var/run/docker.sock "$@"
}

compose() {
  docker_local compose --project-name gpt-demo-services --file "$compose_file" "$@"
}

wait_for_mongo() {
  for ((i = 0; i < 30; i++)); do
    if compose exec -T mongo mongo --quiet --eval \
      'quit(db.adminCommand({ ping: 1 }).ok === 1 ? 0 : 1)' >/dev/null 2>&1; then
      echo "MongoDB ready on 127.0.0.1:${MONGO_DEV_PORT:-27017}"
      return 0
    fi
    sleep 1
  done
  echo 'MongoDB did not become ready; run: bash infra/mongo/mongo.sh logs' >&2
  return 1
}

action=${1:-up}
case "$action" in
  up)
    compose up -d
    wait_for_mongo
    ;;
  down)
    # Docker Compose keeps named volumes unless --volumes is passed.
    compose down
    ;;
  restart)
    compose restart
    wait_for_mongo
    ;;
  status)
    compose ps
    ;;
  logs)
    compose logs --tail=100 "${@:2}" mongo
    ;;
  *)
    echo "usage: $0 [up|down|restart|status|logs]" >&2
    exit 2
    ;;
esac
