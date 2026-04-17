// SPDX-License-Identifier: GPL-2.0
//
// Minimal XDP program whose only role is to put the NIC driver into
// XDP mode so an AF_XDP socket can bind with XDP_ZEROCOPY. Every
// packet is passed to the normal kernel stack; this program does not
// participate in AF_XDP redirection. Used by handoff_demo on bare
// metal where we only exercise TX and therefore don't need a
// dispatcher or xsks_map.

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

SEC("xdp")
int xdp_pass(struct xdp_md *ctx) {
    (void)ctx;
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
