#!/bin/sh
# Builds silver to run on any x86-64 machine at the full speed of its CPU.
#
# The code is built for the portable x86-64 baseline. leanVM's experimental
# prover is built once more per CPU tier (AVX2 up to AVX-512 with GFNI), and
# the fastest tier the CPU runs is picked at run time. leanVM's
# `scripts/tier-rustc`, Cargo's rustc wrapper, compiles each tier with its CPU
# features. A `-C target-cpu=native` build is about as fast, but runs only on
# CPUs like the build machine's.
#
# Usage: scripts/build-prod.sh [cargo build arguments], e.g.
#   scripts/build-prod.sh --features silver/metrics --bin silver
# PROFILE picks the profile (default release-prod); the tiers need opt-level
# 2 or 3. Fails unless target/$PROFILE/silver holds the tiers' code.
set -eu

profile=${PROFILE:-release-prod}

# leanVM's checkout, from the path of its `lean_vm` crate.
manifest=$(cargo metadata --locked --format-version 1 |
    grep -o '"manifest_path":"[^"]*/crates/lean_vm/Cargo.toml"' | head -n 1 | cut -d'"' -f4)
leanvm=${manifest%/crates/lean_vm/Cargo.toml}
if [ ! -f "$leanvm/scripts/tier-rustc" ]; then
    echo "no scripts/tier-rustc in leanVM's checkout at '$leanvm'" >&2
    exit 1
fi

# A fixed, executable path: the tiers rebuild whenever the wrapper's changes.
mkdir -p target
wrapper=$PWD/target/leanvm-tier-rustc
cmp -s "$leanvm/scripts/tier-rustc" "$wrapper" || cp "$leanvm/scripts/tier-rustc" "$wrapper"
chmod +x "$wrapper"

RUSTFLAGS="-C target-cpu=x86-64" RUSTC_WRAPPER="$wrapper" \
    cargo build --profile "$profile" --locked --features silver/leanvm_cpu_tiers "$@"

# GFNI instructions exist only in the tiers' code.
sh "$leanvm/scripts/check-tiers" "target/$profile/silver"
