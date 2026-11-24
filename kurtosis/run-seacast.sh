#!/usr/bin/env bash
# Builds the two images of net-pq-seacast.yaml from local checkouts and runs
# the devnet. The checkouts are siblings of this repository unless set:
#
#   PRYSM          Prysm, on its `seacast-devnet` branch
#   IMAGE_BUILDER  eth-client-docker-image-builder, for silver/Dockerfile.pq-devnet-0
#   PACKAGE        ethereum-package, on its `pq` branch with the Silver launcher
#
# ENCLAVE names the enclave (silver-seacast) and ARGS the network file. An
# enclave of that name is replaced only with FRESH=1.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PRYSM="${PRYSM:-$ROOT/../prysm-private}"
IMAGE_BUILDER="${IMAGE_BUILDER:-$ROOT/../eth-client-docker-image-builder}"
PACKAGE="${PACKAGE:-$ROOT/../ethereum-package}"
ENCLAVE="${ENCLAVE:-silver-seacast}"
ARGS="${ARGS:-$HERE/net-pq-seacast.yaml}"
STAGE="$HERE/.cache/seacast"

for bin in docker go kurtosis sed; do
  command -v "$bin" >/dev/null || { echo "missing dependency: $bin" >&2; exit 1; }
done
for dir in "$PRYSM" "$IMAGE_BUILDER/silver" "$PACKAGE"; do
  [ -d "$dir" ] || { echo "missing checkout: $dir" >&2; exit 1; }
done
# Kurtosis does not find the package through a path with `..` in it.
PACKAGE="$(cd "$PACKAGE" && pwd)"
if kurtosis enclave inspect "$ENCLAVE" >/dev/null 2>&1 && [ "${FRESH:-0}" != 1 ]; then
  echo "enclave $ENCLAVE exists: set FRESH=1 to replace it" >&2
  exit 1
fi

mkdir -p "$STAGE/prysm"

# Silver: the pq-devnet-0 image with the feature on.
sed -e 's|--features silver/metrics |--features silver/metrics,silver/seacast |' \
  "$IMAGE_BUILDER/silver/Dockerfile.pq-devnet-0" > "$STAGE/Dockerfile"
grep -q 'silver/seacast' "$STAGE/Dockerfile" ||
  { echo "silver/Dockerfile.pq-devnet-0 has changed: the edit did not apply" >&2; exit 1; }
COMMIT="$(git -C "$ROOT" rev-parse --short HEAD)"
if [ -n "$(git -C "$ROOT" status --porcelain)" ]; then
  COMMIT="$COMMIT-dirty"
fi
docker buildx build --load --file "$STAGE/Dockerfile" \
  --build-arg "COMMIT_HASH=$COMMIT-seacast" --tag silver:seacast "$ROOT"

# Prysm: its binary built here and laid over the pq-devnet-0 image, which has
# the leanVM sidecar and the wrapper the package starts. The binary links this
# host's glibc, which must not be newer than the image's.
(cd "$PRYSM" && CGO_ENABLED=1 go build -tags=blst_enabled,blst_portable \
  -o "$STAGE/prysm/beacon-chain" ./cmd/beacon-chain)
cat > "$STAGE/prysm/Dockerfile" <<'DOCKERFILE'
FROM ethpandaops/prysm-beacon-chain:pq-devnet-0
COPY --chmod=755 beacon-chain /app/cmd/beacon-chain/beacon-chain
DOCKERFILE
docker build --tag prysm-beacon-chain:seacast-devnet "$STAGE/prysm"

if [ "${FRESH:-0}" = 1 ]; then
  kurtosis enclave rm -f "$ENCLAVE" >/dev/null 2>&1 || true
fi
exec kurtosis run --privileged --enclave "$ENCLAVE" "$PACKAGE" \
  --args-file "$ARGS" --image-download missing
