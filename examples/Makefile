# Build compiled BPF objects used by the Rust examples. The generated
# .o file is gitignored; run `make` here before a bare-metal
# handoff_demo run:
#
#   make -C examples
#   sudo ip link set dev enp8s0 xdpdrv obj examples/xdp_pass.o sec xdp
#
# The handoff_demo / handoff_loss scripts themselves do not call
# clang — they assume the .o is already present when --interface is
# passed.

CLANG  ?= clang
ARCH   := $(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/')
CFLAGS := -target bpf -D__TARGET_ARCH_$(ARCH) -Wall -O2 -g \
          -I/usr/include/$(shell uname -m)-linux-gnu

all: xdp_pass.o

xdp_pass.o: xdp_pass.c
	$(CLANG) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o

.PHONY: all clean
