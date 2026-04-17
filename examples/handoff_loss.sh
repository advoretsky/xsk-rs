#!/usr/bin/env bash
# Runs handoff_demo through repeated SIGUSR1-triggered handoffs and
# collects timing / accounting stats. Fails if any iteration violates
# the issue #117 acceptance target (handoff window >100 ms, or any
# submitted/completed mismatch).
#
# Usage:  sudo examples/handoff_loss.sh [iterations]
# (Must be run as root — veth setup and AF_XDP bind both need it.)

set -eu -o pipefail

ITER=20
IFACE=""
ZEROCOPY=0
# Peer interface whose rx_packets we snapshot to verify wire-level
# delivery. Only meaningful on the veth path (both ends local); on
# a real NIC (--interface NAME) there is no peer whose counters we
# control, so the check is skipped automatically.
VETH_TX_IFACE="hdveth_a"
VETH_RX_IFACE="hdveth_b"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --interface)
            IFACE="$2"
            shift 2
            ;;
        --zero-copy)
            ZEROCOPY=1
            shift
            ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                ITER="$1"
                shift
            else
                echo "usage: $0 [iterations] [--interface NAME] [--zero-copy]" >&2
                exit 2
            fi
            ;;
    esac
done

FORK_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${FORK_DIR}/target/release/examples/handoff_demo"
UDS="/tmp/xsk-handoff.sock"
LOG_DIR="/tmp/handoff_loss_logs"
MAX_HANDOFF_US=100000       # 100 ms
MAX_FAILED_ITERS=0

SERVER_ARGS=(--role server)
if [[ -n "$IFACE" ]]; then
    SERVER_ARGS+=(--interface "$IFACE")
fi
if [[ "$ZEROCOPY" == "1" ]]; then
    SERVER_ARGS+=(--zero-copy)
fi
echo "handoff_loss: ITER=$ITER IFACE='${IFACE:-veth}' ZC=$ZEROCOPY"

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
wire_checked=0
wire_mismatch=0

read_stat() {
    # $1 = ifname, $2 = stat file under statistics/. Missing iface → 0.
    local p="/sys/class/net/$1/statistics/$2"
    if [[ -r "$p" ]]; then
        cat "$p"
    else
        echo 0
    fi
}

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
    if [[ -z "$IFACE" ]]; then
        ip link del hdveth_a 2>/dev/null || true
    fi
    rm -f "$UDS" 2>/dev/null || true
}
trap cleanup EXIT

for i in $(seq 1 "$ITER"); do
    cleanup
    log="${LOG_DIR}/run_${i}.log"

    "$BIN" "${SERVER_ARGS[@]}" >"$log" 2>&1 &
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

    # Wire-level snapshot on the veth path only. On --interface we
    # don't own the peer, so we can't count its rx_packets. Sample
    # entirely before SIGUSR1 because the server tears the veth down
    # on exit — counters evaporate with the interface.
    if [[ -z "$IFACE" ]]; then
        tx_before=$(read_stat "$VETH_TX_IFACE" tx_packets)
        rx_before=$(read_stat "$VETH_RX_IFACE" rx_packets)
    fi

    # Steady-state TX window: let the continuous TX loop run a bit
    # so the counters have non-trivial deltas to compare.
    sleep 0.2

    if [[ -z "$IFACE" ]]; then
        tx_after=$(read_stat "$VETH_TX_IFACE" tx_packets)
        rx_after=$(read_stat "$VETH_RX_IFACE" rx_packets)
        tx_delta=$((tx_after - tx_before))
        rx_delta=$((rx_after - rx_before))
    fi

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

    # Wire-level check: on a veth pair, every packet counted on the
    # server-side tx_packets should also appear in the peer-side
    # rx_packets. Non-atomic counter reads + softirq batching mean
    # the two samples may diverge by a handful of packets even on a
    # healthy run; anything over 1% is the alarming case.
    wire_note=""
    if [[ -z "$IFACE" ]]; then
        wire_checked=$((wire_checked + 1))
        # Absolute difference, in both directions.
        if [[ "$rx_delta" -ge "$tx_delta" ]]; then
            diff=$((rx_delta - tx_delta))
        else
            diff=$((tx_delta - rx_delta))
        fi
        tolerance=$((tx_delta / 100))
        if [[ "$tolerance" -lt 10 ]]; then tolerance=10; fi
        if [[ "$diff" -gt "$tolerance" ]]; then
            wire_mismatch=$((wire_mismatch + 1))
            wire_note=" WIRE_MISMATCH tx=${tx_delta} rx=${rx_delta} (Δ=${diff})"
        else
            wire_note=" wire_tx=${tx_delta} rx=${rx_delta}"
        fi
    fi
    printf "iter %3d: handoff_window=%s (%sµs)%s — OK\n" \
        "$i" "$window" "$window_us" "$wire_note"

    # Give the kernel time to fully release the AF_XDP queue binding
    # before the next iteration tries to bind again — otherwise
    # xsk_socket__create_shared returns EBUSY. Only matters on real
    # NICs; veth releases immediately.
    if [[ -n "$IFACE" ]]; then
        sleep 0.5
    fi
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
if [[ "$wire_checked" -gt 0 ]]; then
    echo "  wire-checked:   $wire_checked (|Δ(tx,rx)| ≤ max(10, tx/100))"
    echo "  wire-mismatch:  $wire_mismatch"
fi
echo "==================================================================="

if [[ "$fail" -gt "$MAX_FAILED_ITERS" ]]; then
    exit 1
fi
if [[ "$wire_mismatch" -gt 0 ]]; then
    echo "wire-level mismatch detected (see iter lines)" >&2
    exit 1
fi
exit 0
