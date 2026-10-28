#!/usr/bin/env bash
# macOS/Linux bootstrap. Existing settings and persistent volumes are retained.
set -euo pipefail
cd "$(dirname "$0")/.."
if ! command -v docker >/dev/null 2>&1; then
  echo "Install Docker Desktop, open it, and retry: bash scripts/bootstrap.sh" >&2
  exit 1
fi
docker info >/dev/null
docker compose version >/dev/null
if [[ ! -f .env ]]; then
  command -v openssl >/dev/null 2>&1 || { echo "openssl is required to generate a local password." >&2; exit 1; }
  secret=$(openssl rand -hex 24)
  (umask 077; sed "s/replace-with-a-local-password/$secret/" .env.example > .env)
fi
# The pinned Spark image and Hadoop archive use the x86 runtime profile.
# Docker Desktop can emulate it on Apple Silicon; performance will vary.
case "$(uname -m)" in
  arm64|aarch64)
    if ! grep -q '^COMPOSE_FILE=' .env; then
      printf '\nCOMPOSE_FILE=compose.yaml:compose.apple.yaml\n' >> .env
    fi
    ;;
esac
mkdir -p data
docker compose config --quiet
if ! docker compose up -d --build; then
  docker compose logs --no-color --tail=40 hdfs-init namenode datanode streaming
  exit 1
fi
echo "Dashboard: http://localhost:8501 | Spark: http://localhost:8080 | HDFS: http://localhost:9870"
