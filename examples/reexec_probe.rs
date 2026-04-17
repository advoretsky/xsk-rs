// Probe AF_XDP FD lifetime semantics to decide re-exec handoff design.
//
// For crouter issue #117: before committing to fork surgery, answer
// empirically which file descriptors must be inherited by a re-exec'd
// successor to keep the kernel UMEM alive and the rings mmappable.
//
// Setup: a veth pair in the init netns. We create one UMEM + one AF_XDP
// socket on the first veth leg via xsk-rs and then run a series of
// probes on the raw kernel FDs.
//
// The probes intentionally bypass xsk-rs to see what the kernel itself
// allows — mmap of each ring offset, getsockopt(XDP_STATISTICS) as a
// lightweight liveness check — both before and after closing the
// libxdp-owned UMEM socket FD.

use std::{
    convert::TryInto,
    env,
    ffi::CString,
    mem::{MaybeUninit, size_of},
    net::Ipv4Addr,
    os::fd::{AsRawFd, RawFd},
    process,
    ptr,
    thread,
    time::Duration,
};
use tokio::runtime::Runtime;
use xsk_rs::{
    Socket, Umem,
    config::{LibxdpFlags, SocketConfig, UmemConfig},
};

// Deliberately duplicated from the library's internal constant to
// avoid exporting it just for this example.
const FRAME_COUNT: u32 = 64;

#[allow(dead_code)]
mod setup;
use setup::{LinkIpAddr, PacketGenerator, VethDevConfig, util, veth_setup};

const SOL_XDP: libc::c_int = 283;
const XDP_MMAP_OFFSETS: libc::c_int = 1;
const XDP_STATISTICS: libc::c_int = 7;

const XDP_PGOFF_RX_RING: libc::off_t = 0;
const XDP_PGOFF_TX_RING: libc::off_t = 0x80000000;
const XDP_UMEM_PGOFF_FILL_RING: libc::off_t = 0x100000000;
const XDP_UMEM_PGOFF_COMPLETION_RING: libc::off_t = 0x180000000;

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
struct XdpRingOffset {
    producer: u64,
    consumer: u64,
    desc: u64,
    flags: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
struct XdpMmapOffsets {
    rx: XdpRingOffset,
    tx: XdpRingOffset,
    fr: XdpRingOffset,
    cr: XdpRingOffset,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
struct XdpStatistics {
    rx_dropped: u64,
    rx_invalid_descs: u64,
    tx_invalid_descs: u64,
    rx_ring_full: u64,
    rx_fill_ring_empty_descs: u64,
    tx_ring_empty_descs: u64,
}

fn getsockopt_offsets(fd: RawFd) -> Result<XdpMmapOffsets, std::io::Error> {
    let mut off = MaybeUninit::<XdpMmapOffsets>::zeroed();
    let mut len = size_of::<XdpMmapOffsets>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            SOL_XDP,
            XDP_MMAP_OFFSETS,
            off.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if rc != 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { off.assume_init() })
    }
}

fn getsockopt_stats(fd: RawFd) -> Result<XdpStatistics, std::io::Error> {
    let mut st = MaybeUninit::<XdpStatistics>::zeroed();
    let mut len = size_of::<XdpStatistics>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            SOL_XDP,
            XDP_STATISTICS,
            st.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if rc != 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { st.assume_init() })
    }
}

fn probe_mmap(fd: RawFd, offset: libc::off_t, len: usize) -> Result<*mut libc::c_void, i32> {
    let addr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd,
            offset,
        )
    };
    if addr == libc::MAP_FAILED {
        Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(-1))
    } else {
        Ok(addr)
    }
}

fn describe_ring(name: &str, ro: &XdpRingOffset) {
    println!(
        "    {name}: producer={:#x} consumer={:#x} desc={:#x} flags={:#x}",
        ro.producer, ro.consumer, ro.desc, ro.flags,
    );
}

fn probe_rings(label: &str, fd: RawFd) {
    println!("\n=== {label} (fd={fd}) ===");
    match getsockopt_stats(fd) {
        Ok(s) => println!("  XDP_STATISTICS: OK rx_dropped={} invalid={}", s.rx_dropped, s.rx_invalid_descs),
        Err(e) => println!("  XDP_STATISTICS: FAIL {}", e),
    }
    let offsets = match getsockopt_offsets(fd) {
        Ok(o) => {
            println!("  XDP_MMAP_OFFSETS: OK");
            describe_ring("rx", &o.rx);
            describe_ring("tx", &o.tx);
            describe_ring("fr", &o.fr);
            describe_ring("cr", &o.cr);
            Some(o)
        }
        Err(e) => {
            println!("  XDP_MMAP_OFFSETS: FAIL {}", e);
            None
        }
    };

    // Page-aligned sizes; these are close enough to see whether mmap
    // rejects on the offset alone vs. rejects on size mismatch.
    let page = 4096usize;
    let try_mmap = |off_name: &str, off: libc::off_t, len: usize| match probe_mmap(fd, off, len) {
        Ok(p) => {
            println!("  mmap({off_name}) OK at {:p}", p);
            unsafe { libc::munmap(p, len) };
        }
        Err(e) => println!("  mmap({off_name}) FAIL errno={}", e),
    };

    // Use a generous ring descriptor count (2048) to size mmap.
    let rx_len = offsets.map(|o| (o.rx.desc + 2048 * 8) as usize).unwrap_or(page);
    let tx_len = offsets.map(|o| (o.tx.desc + 2048 * 8) as usize).unwrap_or(page);
    let fr_len = offsets.map(|o| (o.fr.desc + 2048 * 8) as usize).unwrap_or(page);
    let cr_len = offsets.map(|o| (o.cr.desc + 2048 * 8) as usize).unwrap_or(page);

    try_mmap("XDP_PGOFF_RX_RING", XDP_PGOFF_RX_RING, rx_len);
    try_mmap("XDP_PGOFF_TX_RING", XDP_PGOFF_TX_RING, tx_len);
    try_mmap("XDP_UMEM_PGOFF_FILL_RING", XDP_UMEM_PGOFF_FILL_RING, fr_len);
    try_mmap("XDP_UMEM_PGOFF_COMPLETION_RING", XDP_UMEM_PGOFF_COMPLETION_RING, cr_len);
}

fn run_probes(dev1: (VethDevConfig, PacketGenerator), _dev2: (VethDevConfig, PacketGenerator)) {
    let (umem, _descs) = Umem::new(UmemConfig::default(), 64.try_into().unwrap(), false)
        .expect("failed to create UMEM");

    // Don't let libxdp try to load the default dispatcher — we just
    // want a bound AF_XDP socket; we won't route packets in this test.
    let sock_cfg = SocketConfig::builder()
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .build();

    let (_tx_q, _rx_q, _fq_and_cq) = unsafe {
        Socket::new(
            sock_cfg,
            &umem,
            &dev1.0.if_name().parse().unwrap(),
            0,
        )
    }
    .expect("failed to create AF_XDP socket");

    let umem_fd = umem.fd();
    let socket_fd = _tx_q.fd().as_raw_fd();
    let memfd = umem.memfd();

    println!("\n--- handles ---");
    println!("  memfd       = {memfd}");
    println!("  umem_fd     = {umem_fd} (libxdp-owned UMEM socket)");
    println!("  socket_fd   = {socket_fd} (per-queue data socket)");

    probe_rings("Phase 1a: UMEM socket with both FDs open", umem_fd);
    probe_rings("Phase 1b: data socket with both FDs open", socket_fd);

    // Phase 2: close UMEM socket FD via dup trick — we don't own umem_fd
    // directly (libxdp does via xsk_umem struct), so close via libxdp
    // dropping is not possible here without tearing down the whole Umem.
    // Instead dup the fd, then close the dup; that doesn't kill the
    // kernel binding. To actually close it, we'd need to leak the Umem
    // and raw-close the fd.
    //
    // For a clean test: dup the fd, drop the Umem (which calls
    // xsk_umem__delete -> close(umem_fd)), keep the dup to observe what
    // the kernel does to the data socket.

    let dup_umem = unsafe { libc::dup(umem_fd) };
    assert!(dup_umem >= 0, "dup(umem_fd) failed");
    println!("\n--- dup(umem_fd) = {dup_umem} (holds kernel binding alive) ---");

    // Drop Umem -> xsk_umem__delete -> close original umem_fd
    drop(umem);
    println!("--- dropped Umem (libxdp closed original umem_fd={umem_fd}) ---");

    probe_rings(
        "Phase 2a: UMEM socket via dup'd FD after Umem drop",
        dup_umem,
    );
    probe_rings(
        "Phase 2b: data socket after Umem drop, dup still holding",
        socket_fd,
    );

    // Close the dup too — now NO open reference to umem_fd exists.
    unsafe { libc::close(dup_umem) };
    println!("\n--- closed dup_umem ({dup_umem}) — all UMEM-FD references gone ---");

    probe_rings(
        "Phase 3: data socket after every UMEM-FD reference closed",
        socket_fd,
    );

    // Phase 4: fork+exec a child with only the data socket FD and the
    // memfd inherited. The child should be able to mmap all four rings
    // from the socket FD and access the UMEM memory via the memfd. This
    // is the canonical re-exec handoff test.
    println!("\n--- forking child for exec-based handoff test ---");

    // Clear CLOEXEC on both FDs so exec preserves them.
    clear_cloexec(socket_fd);
    clear_cloexec(memfd);

    // Move them to deterministic fd numbers 3 and 4 for the child.
    let child_socket_fd = unsafe { libc::dup2(socket_fd, 3) };
    let child_memfd = unsafe { libc::dup2(memfd, 4) };
    assert_eq!(child_socket_fd, 3);
    assert_eq!(child_memfd, 4);
    clear_cloexec(3);
    clear_cloexec(4);

    let self_path = env::current_exe().unwrap();
    let argv0 = CString::new(self_path.as_os_str().as_encoded_bytes()).unwrap();
    let arg1 = CString::new("--child").unwrap();

    match unsafe { libc::fork() } {
        -1 => panic!("fork failed: {}", std::io::Error::last_os_error()),
        0 => {
            // Child: exec self in --child mode.
            let argv = [argv0.as_ptr(), arg1.as_ptr(), ptr::null()];
            unsafe { libc::execv(argv0.as_ptr(), argv.as_ptr()) };
            eprintln!("execv failed: {}", std::io::Error::last_os_error());
            process::exit(127);
        }
        pid => {
            let mut status: libc::c_int = 0;
            unsafe { libc::waitpid(pid, &mut status, 0) };
            println!(
                "--- child exited with status {} ---",
                libc::WEXITSTATUS(status)
            );
        }
    }

    thread::sleep(Duration::from_millis(200));
}

fn clear_cloexec(fd: RawFd) {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        panic!("F_GETFD({fd}): {}", std::io::Error::last_os_error());
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) };
    if rc < 0 {
        panic!("F_SETFD({fd}): {}", std::io::Error::last_os_error());
    }
}

fn run_child() {
    use std::os::fd::FromRawFd;

    println!("\n=== child (post-exec) ===");
    let socket_fd: RawFd = 3;
    let memfd: RawFd = 4;

    // Confirm both FDs were inherited.
    for (name, fd) in [("socket_fd", socket_fd), ("memfd", memfd)] {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            println!("  {name}={fd} FAIL: {}", std::io::Error::last_os_error());
        } else {
            println!("  {name}={fd} OK (fd flags = {:#x})", flags);
        }
    }

    // Raw probes (same as Phase 3) first — sanity check that the
    // kernel still sees this FD as a live AF_XDP socket with rings.
    probe_rings("child: raw ring probes on inherited FD", socket_fd);

    // High-level reconstruction using the new fork APIs.
    println!("\n=== child: reconstruct via Umem::from_memfd + Socket::from_raw_fd ===");

    // Adopt the inherited FDs as OwnedFd so the fork manages teardown.
    let memfd_owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(memfd) };
    let socket_owned = unsafe { std::os::fd::OwnedFd::from_raw_fd(socket_fd) };

    let (umem, descs) = Umem::from_memfd(
        UmemConfig::default(),
        FRAME_COUNT.try_into().unwrap(),
        memfd_owned,
    )
    .expect("Umem::from_memfd failed");
    println!("  Umem::from_memfd OK — {} frames", descs.len());

    let sock_cfg = SocketConfig::builder()
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .build();

    let (tx_q, rx_q, fq_and_cq) = unsafe {
        Socket::from_raw_fd(sock_cfg, &umem, socket_owned)
            .expect("Socket::from_raw_fd failed")
    };
    println!("  Socket::from_raw_fd OK");
    let (fq, cq) = fq_and_cq.expect("missing fill/comp queues");
    println!(
        "  TxQueue/RxQueue/FillQueue/CompQueue alive — new data-socket FD = {}",
        tx_q.fd().as_raw_fd()
    );

    // Try a getsockopt via the xsk-rs Fd wrapper just to exercise it.
    match tx_q.fd().xdp_statistics() {
        Ok(_) => println!("  Fd::xdp_statistics OK on reconstructed socket"),
        Err(e) => println!("  Fd::xdp_statistics FAIL: {e}"),
    }

    // Explicit drop order: queues first, then umem.
    drop(cq);
    drop(fq);
    drop(rx_q);
    drop(tx_q);
    drop(umem);
    println!("  child: reconstructed queues + UMEM dropped cleanly");
}

fn main() {
    env_logger::init();

    // Re-exec'd child path: just probe the inherited FDs and exit.
    if env::args().any(|a| a == "--child") {
        run_child();
        process::exit(0);
    }

    let dev1_config = VethDevConfig {
        if_name: "xskexp_a".into(),
        addr: [0xf6, 0xe0, 0xf6, 0xc9, 0x60, 0x0a],
        ip_addr: LinkIpAddr::new(Ipv4Addr::new(192, 168, 169, 1), 24),
    };
    let dev2_config = VethDevConfig {
        if_name: "xskexp_b".into(),
        addr: [0x4a, 0xf1, 0x30, 0xeb, 0x0d, 0x31],
        ip_addr: LinkIpAddr::new(Ipv4Addr::new(192, 168, 169, 2), 24),
    };

    let ctrl_c_events = util::ctrl_channel().unwrap();
    let (complete_tx, complete_rx) = crossbeam_channel::bounded(1);

    let runtime = Runtime::new().unwrap();
    let handle = thread::spawn(move || {
        let r = runtime.block_on(veth_setup::run_with_veth_pair(
            dev1_config,
            dev2_config,
            run_probes,
        ));
        let _ = complete_tx.send(());
        r
    });

    crossbeam_channel::select! {
        recv(complete_rx) -> _ => {},
        recv(ctrl_c_events) -> _ => println!("SIGINT received"),
    }

    handle.join().unwrap().unwrap();
    println!("\n=== probe complete ===");
}
