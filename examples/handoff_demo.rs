// Zero-loss AF_XDP handoff demo between two separate processes.
//
// Implements the flag-driven parallel-process takeover model for
// crouter issue #117:
//
//   Process S (server):  cargo run --example handoff_demo -- --role server
//                        Sets up a veth pair, creates a UMEM + AF_XDP
//                        socket, binds a UDS at HANDOFF_UDS, and waits
//                        for a successor to connect.
//
//   Process R (receiver): cargo run --example handoff_demo -- --handoff-from <path>
//                         Connects to the UDS, receives the per-queue
//                         bundle via SCM_RIGHTS, reconstructs the
//                         UMEM+socket via Umem::from_memfd and
//                         Socket::from_raw_fd, and replies with a
//                         READY message.
//
// Stage 1 (this file): proves the bundle transfer + reconstruction
// between two distinct processes. No TX traffic, no loss measurement,
// no SIGUSR1 yet — those land in later commits.

use std::{
    convert::TryInto,
    env,
    io::{self, ErrorKind, IoSlice, IoSliceMut, Read, Write},
    mem::{MaybeUninit, size_of},
    net::Ipv4Addr,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    os::unix::net::{UnixListener, UnixStream},
    path::PathBuf,
    process, ptr,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use xsk_rs::{
    CompQueue, FrameDesc, Socket, TxQueue, Umem,
    config::{BindFlags, LibxdpFlags, SocketConfig, UmemConfig},
};

#[allow(dead_code)]
mod setup;
use setup::{LinkIpAddr, PacketGenerator, VethDevConfig, util, veth_setup};

const HANDOFF_UDS: &str = "/tmp/xsk-handoff.sock";
const FRAME_COUNT: u32 = 64;
const BURST_PACKETS: usize = 1000;
const BURST_BATCH: usize = 16;

// Signal-driven handoff: when the user sends SIGUSR1, the server
// spawns its own successor and transfers state over the UDS. The
// handler is async-signal-safe (just an atomic flip).
static HANDOFF_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sigusr1(_sig: libc::c_int) {
    HANDOFF_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_sigusr1_handler() {
    unsafe {
        libc::signal(libc::SIGUSR1, handle_sigusr1 as libc::sighandler_t);
    }
}

// Minimal ethernet frame: 6 dst + 6 src + 2 ethertype + payload padding.
// Not routable — veth peer will toss it, but we only care that the
// kernel accepts it into the TX path and delivers a completion.
const TEST_FRAME: [u8; 64] = [
    0x02, 0x00, 0x00, 0x00, 0x00, 0x02, // dst MAC (locally-administered)
    0x02, 0x00, 0x00, 0x00, 0x00, 0x01, // src MAC
    0x08, 0x00, // ethertype IPv4
    // 50 bytes of "handoff-demo" padding
    b'h', b'a', b'n', b'd', b'o', b'f', b'f', b'-', b'd', b'e',
    b'm', b'o', b'-', b'p', b'k', b't', b'-', b'-', b'-', b'-',
    b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-',
    b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-',
    b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-', b'-',
];

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct HandoffPayload {
    frame_count: u32,
    // Ring sizes captured at server startup so the receiver builds
    // ring wrappers matching what the kernel already has.
    rx_ring_size: u32,
    tx_ring_size: u32,
    // Reserved for future bundle fields (in-flight FrameDesc snapshot,
    // cursor positions). Kept here so the wire format stays stable.
    _reserved: u32,
}

unsafe impl Send for HandoffPayload {}

#[derive(Debug, Clone)]
struct ServerOpts {
    interface: Option<String>,
    zero_copy: bool,
}

enum Role {
    Server(ServerOpts),
    Receiver(PathBuf),
}

fn parse_role() -> Role {
    let mut opts = ServerOpts {
        interface: None,
        zero_copy: false,
    };
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--handoff-from" => {
                let p = args.next().expect("--handoff-from needs a path argument");
                return Role::Receiver(PathBuf::from(p));
            }
            "--role" => {
                let v = args.next().expect("--role needs a value");
                if v != "server" {
                    eprintln!("unknown --role {v}, expected 'server'");
                    process::exit(2);
                }
            }
            "--interface" => {
                opts.interface = Some(args.next().expect("--interface needs a name"));
            }
            "--zero-copy" => {
                opts.zero_copy = true;
            }
            other => {
                eprintln!("unknown argument {other}");
                process::exit(2);
            }
        }
    }
    Role::Server(opts)
}

fn main() {
    env_logger::init();
    match parse_role() {
        Role::Server(opts) => run_server(opts),
        Role::Receiver(path) => run_receiver(path),
    }
}

// ---------------------- server side ----------------------

fn run_server(opts: ServerOpts) {
    // Pre-bind the UDS before spawning the veth/AF_XDP setup so a
    // concurrently started receiver never loses its connect() race.
    let _ = std::fs::remove_file(HANDOFF_UDS);
    let listener = UnixListener::bind(HANDOFF_UDS).expect("bind UDS");
    println!("server listening on {HANDOFF_UDS}");

    if let Some(iface) = opts.interface.clone() {
        // Bare-metal path: the NIC already exists, no veth setup.
        // We don't bring XDP up ourselves — the test harness does so
        // once, outside the iteration loop, to avoid carrier drops.
        server_body_iface(iface, opts.zero_copy, listener);
        let _ = std::fs::remove_file(HANDOFF_UDS);
        println!("server exited");
        return;
    }

    // Veth sandbox path (default).
    let dev1 = VethDevConfig {
        if_name: "hdveth_a".into(),
        addr: [0xf6, 0xe0, 0xf6, 0xc9, 0x60, 0x1a],
        ip_addr: LinkIpAddr::new(Ipv4Addr::new(192, 168, 170, 1), 24),
    };
    let dev2 = VethDevConfig {
        if_name: "hdveth_b".into(),
        addr: [0x4a, 0xf1, 0x30, 0xeb, 0x0d, 0x41],
        ip_addr: LinkIpAddr::new(Ipv4Addr::new(192, 168, 170, 2), 24),
    };

    let ctrl_c_events = util::ctrl_channel().unwrap();
    let (complete_tx, complete_rx) = crossbeam_channel::bounded(1);
    let runtime = Runtime::new().unwrap();

    let handle = thread::spawn(move || {
        let r = runtime.block_on(veth_setup::run_with_veth_pair(dev1, dev2, move |d1, _d2| {
            server_body(d1, listener)
        }));
        let _ = complete_tx.send(());
        r
    });

    crossbeam_channel::select! {
        recv(complete_rx) -> _ => {},
        recv(ctrl_c_events) -> _ => println!("SIGINT received"),
    }

    handle.join().unwrap().unwrap();
    let _ = std::fs::remove_file(HANDOFF_UDS);
    println!("server exited");
}

fn server_body_iface(interface: String, zero_copy: bool, listener: UnixListener) {
    install_sigusr1_handler();

    // igb ZEROCOPY requires frame_headroom=64 and default frame_size
    // (see crouter CLAUDE.md). UmemConfig::default has both.
    let (umem, descs) = Umem::new(
        UmemConfig::default(),
        FRAME_COUNT.try_into().unwrap(),
        false,
    )
    .expect("Umem::new");

    let mut builder = SocketConfig::builder();
    builder.libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD);
    if zero_copy {
        builder.bind_flags(BindFlags::XDP_ZEROCOPY | BindFlags::XDP_USE_NEED_WAKEUP);
    }
    let sock_cfg = builder.build();

    let (mut tx_q, _rx_q, fq_cq) = unsafe {
        Socket::new(
            sock_cfg,
            &umem,
            &interface.parse().unwrap(),
            0,
        )
    }
    .expect("Socket::new on interface (is XDP attached for ZEROCOPY?)");
    let (_fq, mut cq) = fq_cq.expect("fill/comp queues present");

    run_server_loop(
        umem,
        descs,
        tx_q,
        cq,
        sock_cfg,
        listener,
        format!("bare-metal {interface} zero_copy={zero_copy}"),
    )
}

fn server_body(dev: (VethDevConfig, PacketGenerator), listener: UnixListener) {
    install_sigusr1_handler();

    let (umem, descs) = Umem::new(
        UmemConfig::default(),
        FRAME_COUNT.try_into().unwrap(),
        false,
    )
    .expect("Umem::new");

    let sock_cfg = SocketConfig::builder()
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .build();

    let (tx_q, _rx_q, fq_cq) = unsafe {
        Socket::new(
            sock_cfg,
            &umem,
            &dev.0.if_name().parse().unwrap(),
            0,
        )
    }
    .expect("Socket::new");
    let (_fq, cq) = fq_cq.expect("fill/comp queues present");

    run_server_loop(umem, descs, tx_q, cq, sock_cfg, listener, "veth".into())
}

fn run_server_loop(
    umem: Umem,
    descs: Vec<FrameDesc>,
    mut tx_q: TxQueue,
    mut cq: CompQueue,
    sock_cfg: SocketConfig,
    listener: UnixListener,
    tag: String,
) {
    let socket_fd = tx_q.fd().as_raw_fd();
    let memfd = umem.memfd();
    let rx_ring_size = sock_cfg.rx_queue_size().get();
    let tx_ring_size = sock_cfg.tx_queue_size().get();

    println!(
        "server[{tag}]: UMEM ready ({} frames), socket_fd={socket_fd}, memfd={memfd}",
        descs.len()
    );
    println!("server: send SIGUSR1 to trigger handoff (pid={})", process::id());

    // Non-blocking so the TX loop can poll both signal state and
    // (once we've spawned a successor) the incoming UDS connection.
    listener.set_nonblocking(true).expect("listener nonblocking");

    let mut free: Vec<FrameDesc> = descs;
    let mut submitted: u64 = 0;
    let mut completed: u64 = 0;

    // --- phase 1: steady-state TX until SIGUSR1 ---
    while !HANDOFF_REQUESTED.load(Ordering::SeqCst) {
        tx_step(&umem, &mut tx_q, &mut cq, &mut free, &mut submitted, &mut completed);
    }

    let handoff_start = Instant::now();
    println!(
        "server: SIGUSR1 received after submitted={submitted} completed={completed}; \
         spawning successor"
    );

    // Spawn the successor as a sibling process. It starts from scratch
    // and connects back to our UDS — no FD inheritance needed.
    let self_exe = env::current_exe().expect("current_exe");
    let child = std::process::Command::new(&self_exe)
        .arg("--handoff-from")
        .arg(HANDOFF_UDS)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("spawn successor");
    println!("server: spawned successor pid={}", child.id());

    // --- phase 2: keep TX flowing while the successor starts, then
    // accept its connection when it's ready. This is the key zero-loss
    // ingredient — no idle gap while the new process initializes. ---
    let client = loop {
        tx_step(&umem, &mut tx_q, &mut cq, &mut free, &mut submitted, &mut completed);
        match listener.accept() {
            Ok((s, _)) => break s,
            Err(e) if e.kind() == ErrorKind::WouldBlock => continue,
            Err(e) => panic!("accept: {e}"),
        }
    };
    let t_connected = handoff_start.elapsed();
    println!(
        "server: successor connected after {:?} \
         (submitted={submitted} completed={completed})"
        , t_connected
    );

    // --- phase 3: quiesce — no more submissions; drain completions. ---
    let drain_start = Instant::now();
    while completed < submitted {
        drain_step(&mut cq, &mut free, &mut completed);
    }
    let t_drain = drain_start.elapsed();
    println!(
        "server: TX drained in {:?} (final submitted={submitted} completed={completed})",
        t_drain
    );

    // --- phase 4: bundle + READY handshake. ---
    let payload = HandoffPayload {
        frame_count: FRAME_COUNT,
        rx_ring_size,
        tx_ring_size,
        _reserved: 0,
    };

    send_bundle(&client, &[socket_fd, memfd], &payload).expect("send_bundle");

    let mut client = client; // rebind mutable
    let mut buf = [0u8; 16];
    let n = client.read(&mut buf).expect("read READY");
    println!(
        "server: ack={:?} handoff_window={:?}",
        std::str::from_utf8(&buf[..n]).unwrap_or("<non-utf8>"),
        handoff_start.elapsed()
    );
    drop(client);

    // We wait for the child to exit so its output is captured before
    // we return and the veth fixture is torn down.
    let status = child.wait_with_output().expect("wait child");
    println!(
        "server: successor exited with status={} stdout_lines={}",
        status.status,
        status.stdout.split(|b| *b == b'\n').count()
    );
}

// Try to submit one batch and reap completions; non-blocking, returns
// quickly if nothing to do.
fn tx_step(
    umem: &Umem,
    tx_q: &mut TxQueue,
    cq: &mut CompQueue,
    free: &mut Vec<FrameDesc>,
    submitted: &mut u64,
    completed: &mut u64,
) {
    use std::io::Write as _;
    let to_send = BURST_BATCH.min(free.len());
    if to_send > 0 {
        let mut batch: Vec<FrameDesc> = free.drain(..to_send).collect();
        for d in &mut batch {
            unsafe {
                umem.data_mut(d).cursor().write_all(&TEST_FRAME).unwrap();
            }
        }
        let sent = unsafe { tx_q.produce_and_wakeup(&batch).unwrap() };
        *submitted += sent as u64;
        if sent < batch.len() {
            free.extend(batch.into_iter().skip(sent));
        }
    }
    let mut scratch = [FrameDesc::default(); BURST_BATCH];
    let got = unsafe { cq.consume(&mut scratch) };
    if got > 0 {
        *completed += got as u64;
        for d in &scratch[..got] {
            free.push(*d);
        }
    }
}

fn drain_step(cq: &mut CompQueue, free: &mut Vec<FrameDesc>, completed: &mut u64) {
    let mut scratch = [FrameDesc::default(); BURST_BATCH];
    let got = unsafe { cq.consume(&mut scratch) };
    if got > 0 {
        *completed += got as u64;
        for d in &scratch[..got] {
            free.push(*d);
        }
    } else {
        thread::sleep(Duration::from_micros(50));
    }
}

// ---------------------- receiver side ----------------------

fn run_receiver(path: PathBuf) {
    let t0 = Instant::now();
    println!("receiver: pid={} connecting to {}", process::id(), path.display());
    // The server is running TX while we come up; it will accept our
    // connection within its next loop iteration. A small retry loop
    // covers the race where we hit the socket before the server
    // finished binding.
    let mut stream = loop {
        match UnixStream::connect(&path) {
            Ok(s) => break s,
            Err(e) if e.kind() == ErrorKind::NotFound
                || e.kind() == ErrorKind::ConnectionRefused =>
            {
                thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("connect UDS: {e}"),
        }
    };
    println!("receiver: UDS connected in {:?}", t0.elapsed());

    let (fds, payload) = recv_bundle(&stream, 2).expect("recv_bundle");
    assert_eq!(fds.len(), 2, "expected 2 FDs (socket, memfd)");
    println!(
        "receiver: got bundle: frames={} rx={} tx={} fds={:?}",
        payload.frame_count,
        payload.rx_ring_size,
        payload.tx_ring_size,
        fds.iter().map(|f| f.as_raw_fd()).collect::<Vec<_>>(),
    );

    // The first fd in the bundle is the AF_XDP socket; the second is
    // the UMEM memfd. Take them by value; OwnedFd will close on drop.
    let mut fds_iter = fds.into_iter();
    let socket_fd = fds_iter.next().unwrap();
    let memfd = fds_iter.next().unwrap();

    let (umem, mut descs) = Umem::from_memfd(
        UmemConfig::default(),
        payload.frame_count.try_into().unwrap(),
        memfd,
    )
    .expect("Umem::from_memfd");
    println!("receiver: Umem::from_memfd OK — {} frames", descs.len());

    let sock_cfg = SocketConfig::builder()
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .rx_queue_size(
            xsk_rs::config::QueueSize::new(payload.rx_ring_size).expect("rx ring size"),
        )
        .tx_queue_size(
            xsk_rs::config::QueueSize::new(payload.tx_ring_size).expect("tx ring size"),
        )
        .build();

    let (mut tx_q, rx_q, fq_cq) = unsafe {
        Socket::from_raw_fd(sock_cfg, &umem, socket_fd).expect("Socket::from_raw_fd")
    };
    let (fq, mut cq) = fq_cq.expect("fill/comp queues");
    println!(
        "receiver: Socket::from_raw_fd OK (new fd={})",
        tx_q.fd().as_raw_fd()
    );

    match tx_q.fd().xdp_statistics() {
        Ok(_) => println!("receiver: xdp_statistics OK on reconstructed socket"),
        Err(e) => println!("receiver: xdp_statistics FAIL: {e}"),
    }

    // The server drained its TX ring before handoff, so every frame
    // in `descs` is free. Run a burst on the reconstructed rings to
    // prove TX still flows, and to generate traffic that the loss
    // harness can measure.
    let mut free: Vec<FrameDesc> = descs.drain(..).collect();
    let tx_start = Instant::now();
    let (post_sub, post_cmp) = tx_burst(&umem, &mut tx_q, &mut cq, &mut free, BURST_PACKETS)
        .expect("receiver post-handoff tx_burst");
    println!(
        "receiver: post-handoff tx_burst submitted={post_sub} completed={post_cmp} elapsed={:?}",
        tx_start.elapsed()
    );
    assert_eq!(post_sub, post_cmp, "receiver: TX accounting mismatch");

    // Signal to the server that we have full possession — the server
    // kept TX flowing until it accepted our connection, so the
    // kernel-visible "outage" is just the quiesce + bundle exchange.
    stream.write_all(b"READY").expect("send READY");
    println!(
        "receiver: READY sent; total reconstruction+burst took {:?}",
        t0.elapsed()
    );

    drop(cq);
    drop(fq);
    drop(rx_q);
    drop(tx_q);
    drop(umem);
}

// ---------------------- shared TX burst ----------------------
//
// Drive the AF_XDP TX ring for `n_pkts` submissions, reclaiming
// completions as they arrive. Returns (submitted, completed). If the
// ring is healthy both numbers match `n_pkts`.

fn tx_burst(
    umem: &Umem,
    tx_q: &mut TxQueue,
    cq: &mut CompQueue,
    free: &mut Vec<FrameDesc>,
    n_pkts: usize,
) -> io::Result<(usize, usize)> {
    use std::io::Write as _;

    let mut submitted = 0usize;
    let mut completed = 0usize;
    let mut scratch = [FrameDesc::default(); BURST_BATCH];

    while completed < n_pkts {
        // Submit a batch of frames from the free pool.
        let to_send = (n_pkts - submitted).min(free.len()).min(BURST_BATCH);
        if to_send > 0 {
            let mut batch: Vec<FrameDesc> = free.drain(..to_send).collect();
            for d in &mut batch {
                unsafe {
                    umem.data_mut(d).cursor().write_all(&TEST_FRAME)?;
                }
            }
            let sent = unsafe { tx_q.produce_and_wakeup(&batch)? };
            submitted += sent;
            if sent < batch.len() {
                // Any frame rejected by the ring goes back to free.
                free.extend(batch.into_iter().skip(sent));
            }
        }

        // Reap completions.
        let got = unsafe { cq.consume(&mut scratch) };
        if got > 0 {
            completed += got;
            for d in &scratch[..got] {
                free.push(*d);
            }
        }

        if to_send == 0 && got == 0 {
            thread::sleep(Duration::from_micros(100));
        }
    }
    Ok((submitted, completed))
}

// ---------------------- SCM_RIGHTS wire helpers ----------------------

fn send_bundle(
    stream: &UnixStream,
    fds: &[RawFd],
    payload: &HandoffPayload,
) -> io::Result<()> {
    let payload_bytes = unsafe {
        std::slice::from_raw_parts(payload as *const _ as *const u8, size_of::<HandoffPayload>())
    };

    // Build an iovec carrying the payload.
    let iov = [IoSlice::new(payload_bytes)];
    let mut msg: libc::msghdr = unsafe { MaybeUninit::zeroed().assume_init() };
    msg.msg_iov = iov.as_ptr() as *mut _;
    msg.msg_iovlen = iov.len() as _;

    // Ancillary buffer large enough to hold `fds.len()` FDs.
    let cmsg_space = unsafe { libc::CMSG_SPACE((fds.len() * size_of::<RawFd>()) as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut _;
    msg.msg_controllen = cmsg_space as _;

    // Fill the cmsghdr with SCM_RIGHTS.
    unsafe {
        let cmsg_ptr = libc::CMSG_FIRSTHDR(&msg);
        assert!(!cmsg_ptr.is_null(), "CMSG_FIRSTHDR null");
        (*cmsg_ptr).cmsg_level = libc::SOL_SOCKET;
        (*cmsg_ptr).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg_ptr).cmsg_len = libc::CMSG_LEN((fds.len() * size_of::<RawFd>()) as u32) as _;

        let data_ptr = libc::CMSG_DATA(cmsg_ptr) as *mut RawFd;
        for (i, fd) in fds.iter().enumerate() {
            ptr::write(data_ptr.add(i), *fd);
        }
    }

    let sent = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if sent < 0 {
        Err(io::Error::last_os_error())
    } else if (sent as usize) != payload_bytes.len() {
        Err(io::Error::other(format!(
            "short sendmsg: {sent} of {}",
            payload_bytes.len()
        )))
    } else {
        Ok(())
    }
}

fn recv_bundle(
    stream: &UnixStream,
    expected_fd_count: usize,
) -> io::Result<(Vec<OwnedFd>, HandoffPayload)> {
    let mut payload = HandoffPayload::default();
    let payload_bytes = unsafe {
        std::slice::from_raw_parts_mut(
            &mut payload as *mut _ as *mut u8,
            size_of::<HandoffPayload>(),
        )
    };

    let mut iov = [IoSliceMut::new(payload_bytes)];
    let mut msg: libc::msghdr = unsafe { MaybeUninit::zeroed().assume_init() };
    msg.msg_iov = iov.as_mut_ptr() as *mut _;
    msg.msg_iovlen = iov.len() as _;

    let cmsg_space =
        unsafe { libc::CMSG_SPACE((expected_fd_count * size_of::<RawFd>()) as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut _;
    msg.msg_controllen = cmsg_space as _;

    let rc = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    if (rc as usize) != size_of::<HandoffPayload>() {
        return Err(io::Error::other(format!(
            "short recvmsg: {rc} of {}",
            size_of::<HandoffPayload>()
        )));
    }
    if (msg.msg_flags & libc::MSG_CTRUNC) != 0 {
        return Err(io::Error::other(
            "ancillary data truncated; FD bundle incomplete",
        ));
    }

    let mut fds: Vec<OwnedFd> = Vec::with_capacity(expected_fd_count);
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(cmsg) as *const RawFd;
                let count = ((*cmsg).cmsg_len as usize
                    - (libc::CMSG_LEN(0) as usize))
                    / size_of::<RawFd>();
                for i in 0..count {
                    let raw = ptr::read(data.add(i));
                    fds.push(OwnedFd::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }

    if fds.len() != expected_fd_count {
        return Err(io::Error::other(format!(
            "expected {} FDs, received {}",
            expected_fd_count,
            fds.len()
        )));
    }

    Ok((fds, payload))
}

// Silence unused-import warnings on some configurations.
const _: fn() = || {
    let _ = Duration::from_secs(0);
};
