#!/usr/bin/env bash
set -e
if [[ " $@ " =~ --target' '([^ ]+) ]]; then
  TARGET="${BASH_REMATCH[1]}"
  if [[ " $@ " =~ " --release " ]]; then
    DIR="./target/$TARGET/release"
  elif [[ " $@ " =~ " --profile opt " ]]; then
    DIR="./target/$TARGET/opt"
  else
    DIR="./target/$TARGET/debug"
  fi
else
  TARGET=""
  if [[ " $@ " =~ " --release " ]]; then
    DIR="./target/release"
  elif [[ " $@ " =~ " --profile opt " ]]; then
    DIR="./target/$TARGET/opt"
  else
    DIR="./target/debug"
  fi
fi

if [ "$TARGET" = "" ]; then
  printf "Target: [not specified]\n" 1>&2
  RUNNER=()
elif [ "$TARGET" = $(rustc -vV | awk '/^host/ { print $2 }') ]; then
  printf "Target: [host]\n" 1>&2
  RUNNER=()
elif [ "$TARGET" = "aarch64-unknown-linux-gnu" ]; then
  printf "Target: $TARGET\n" 1>&2
  QEMU_LD_PREFIX="/usr/aarch64-linux-gnu"
  RUNNER=("qemu-aarch64-static")
elif [ "$TARGET" = "riscv64gc-unknown-linux-gnu" ]; then
  printf "Target: $TARGET\n" 1>&2
  QEMU_LD_PREFIX="/usr/riscv64-linux-gnu"
  RUNNER=("qemu-riscv64-static")
else
  printf "Unknown target: $TARGET\n" 1>&2
  exit 1
fi

code=$(mktemp)
chmod 700 $code
CONTROL_FILEPATH="./vectors.control" SO_FILEPATH="$DIR/libvectors.so" $(dirname "$0")/schema-codegen.sh >> $code

PGRX_EMBED=$code cargo rustc --bin pgrx_embed_vectors "$@" -- --cfg pgrx_embed

CARGO_PKG_VERSION="0.0.0" QEMU_LD_PREFIX=$QEMU_LD_PREFIX "${RUNNER[@]}" "$DIR/pgrx_embed_vectors" | expand -t 4
