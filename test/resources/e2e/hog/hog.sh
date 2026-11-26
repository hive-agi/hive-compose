#!/bin/sh
# hog: a fixture that allocates memory until it holds HOG_TARGET_MB MiB,
# HOG_STEP_MB at a time, then idles (or exits 0 when HOG_EXIT=1).
#
# Under a cap lower than the target it is OOM-killed: docker reports
# State.OOMKilled=true and exit 137; a systemd scope with MemoryMax= reports
# result 'oom-kill'. That is the S13b fixture (hc-16c): a 256m cap, a 512 MiB
# target, so the kill is certain and fast. Above the cap it would pass.
#
# POSIX sh + busybox awk only, so it runs in busybox:1.36 with nothing built.
# awk holds the memory: one HOG_STEP_MB chunk is built by doubling, then
# appended to the held string each step. Every step prints one line, so the
# last line before the kill tells how far it got:
#   hog: holding <n> MiB
# and on success, `hog: done <n> MiB`.
#
# Measured (busybox:1.36, cgroup v2, mem_limit=memswap_limit=256m): awk copies
# the held string on each append, so the peak is about twice what it holds.
# The kill comes at about 112-128 MiB held; a 96 MiB target passes (exit 0).
# The 512 MiB default is therefore killed every time, after 7-8 lines.
set -eu
TARGET="${HOG_TARGET_MB:-512}"
STEP="${HOG_STEP_MB:-16}"
PAUSE_MS="${HOG_PAUSE_MS:-50}"
echo "hog: target ${TARGET} MiB in ${STEP} MiB steps (cap is whatever the runtime set)"
awk -v target="$TARGET" -v step="$STEP" -v pause_ms="$PAUSE_MS" 'BEGIN {
  c = "x"; want = step * 1048576
  while (length(c) < want) c = c c
  c = substr(c, 1, want)
  held = ""
  for (n = step; n <= target; n += step) {
    held = held c
    printf "hog: holding %d MiB\n", n; fflush()
    if (pause_ms > 0) system("sleep " pause_ms / 1000)
  }
  printf "hog: done %d MiB\n", length(held) / 1048576; fflush()
}'
if [ "${HOG_EXIT:-0}" = 1 ]; then exit 0; fi
exec sleep "${HOG_IDLE_S:-1200}"
