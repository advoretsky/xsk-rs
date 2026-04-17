#!/usr/bin/env bash
# Runs handoff_demo through repeated SIGUSR1-triggered handoffs and
# collects timing / accounting stats. Fails if any iteration violates
# the issue #117 acceptance target (handoff window >100 ms, or any
# submitted/completed mismatch).
#
# Usage:  sudo examples/handoff_loss.sh [iterations]
# (Must be run as root — veth setup and AF_XDP bind both need it.)

set -eu -o pipefail

ITER="${1:-20}"
FORK_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${FORK_DIR}/target/release/examples/handoff_demo"
UDS="/tmp/xsk-handoff.sock"
LOG_DIR="/tmp/handoff_loss_logs"
MAX_HANDOFF_US=100000       # 100 ms
MAX_FAILED_ITERS=0

if [[ ! -x "$BIN" ]]; then
    echo "building handoff_demo…" >&2
    ( cd "$FORK_DIR" && cargo build --release --example handoff_demo >/dev/null )
fi

mkdir -p "$LOG_DIR"
rm -f "${LOG_DIR}"/*.log

pass=0
fail=0
worst_us=0
sum_us=0

# Timing parser: handoff_window is printed as e.g. "2.328815ms" or
# "345.6µs" or "1.234s". Normalize to microseconds.
parse_us() {
    local s="$1"
    local n="${s%[a-zµ]*}"
    case "$s" in
        *ns)  awk -v n="$n" 'BEGIN{printf "%d", n / 1000}' ;;
        *µs)  awk -v n="$n" 'BEGIN{printf "%d", n}' ;;
        *us)  awk -v n="$n" 'BEGIN{printf "%d", n}' ;;
        *ms)  awk -v n="$n" 'BEGIN{printf "%d", n * 1000}' ;;
        *s)   awk -v n="$n" 'BEGIN{printf "%d", n * 1000000}' ;;
        *)    echo 0 ;;
    esac
}

cleanup() {
    # Best effort; ignore failures.
    ip link del hdveth_a 2>/dev/null || true
    rm -f "$UDS" 2>/dev/null || true
}
trap cleanup EXIT

for i in $(seq 1 "$ITER"); do
    cleanup
    log="${LOG_DIR}/run_${i}.log"

    "$BIN" --role server >"$log" 2>&1 &
    server_wrapper=$!

    # Wait until the server has entered its TX loop.
    for _ in $(seq 1 200); do
        if grep -q "send SIGUSR1 to trigger handoff" "$log" 2>/dev/null; then
            break
        fi
        sleep 0.01
    done

    pid=$(awk -F'[=)]' '/send SIGUSR1 to trigger handoff/{print $(NF-1)}' "$log")
    if [[ -z "$pid" ]]; then
        echo "iter $i: server failed to start — see $log" >&2
        fail=$((fail + 1))
        kill "$server_wrapper" 2>/dev/null || true
        wait "$server_wrapper" 2>/dev/null || true
        continue
    fi

    # Give it a little steady-state TX time.
    sleep 0.2
    kill -USR1 "$pid"

    # Wait for the server to exit.
    wait "$server_wrapper" 2>/dev/null || true

    # Sanity check the log.
    if ! grep -q "ack=\"READY\"" "$log"; then
        echo "iter $i: no READY ack — see $log" >&2
        fail=$((fail + 1))
        continue
    fi

    post_line=$(grep "post-handoff tx_burst" "$log" | head -1 || true)
    sub=$(echo "$post_line" | sed -nE 's/.*submitted=([0-9]+).*/\1/p')
    cmp=$(echo "$post_line" | sed -nE 's/.*completed=([0-9]+).*/\1/p')
    if [[ "$sub" != "$cmp" || -z "$sub" ]]; then
        echo "iter $i: post-handoff submit/complete mismatch sub=$sub cmp=$cmp" >&2
        fail=$((fail + 1))
        continue
    fi

    window=$(grep -oE 'handoff_window=[^ ]+' "$log" | head -1 | cut -d= -f2)
    window_us=$(parse_us "$window")
    if [[ "$window_us" -gt "$MAX_HANDOFF_US" ]]; then
        echo "iter $i: handoff_window $window (${window_us}µs) exceeds ${MAX_HANDOFF_US}µs" >&2
        fail=$((fail + 1))
        continue
    fi

    pass=$((pass + 1))
    sum_us=$((sum_us + window_us))
    if [[ "$window_us" -gt "$worst_us" ]]; then
        worst_us="$window_us"
    fi
    printf "iter %3d: handoff_window=%s (%sµs) — OK\n" "$i" "$window" "$window_us"
done

echo
echo "====================== handoff_loss summary ======================"
echo "  iterations:     $ITER"
echo "  passed:         $pass"
echo "  failed:         $fail"
if [[ "$pass" -gt 0 ]]; then
    avg=$((sum_us / pass))
    echo "  avg window:     ${avg}µs"
    echo "  worst window:   ${worst_us}µs"
fi
echo "==================================================================="

if [[ "$fail" -gt "$MAX_FAILED_ITERS" ]]; then
    exit 1
fi
exit 0
