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
    io::{self, IoSlice, IoSliceMut, Read, Write},
    mem::{MaybeUninit, size_of},
    net::Ipv4Addr,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    os::unix::net::{UnixListener, UnixStream},
    path::PathBuf,
    process, ptr, thread,
    time::Duration,
};
use tokio::runtime::Runtime;
use xsk_rs::{
    CompQueue, FrameDesc, Socket, TxQueue, Umem,
    config::{LibxdpFlags, SocketConfig, UmemConfig},
};

#[allow(dead_code)]
mod setup;
use setup::{LinkIpAddr, PacketGenerator, VethDevConfig, util, veth_setup};

const HANDOFF_UDS: &str = "/tmp/xsk-handoff.sock";
const FRAME_COUNT: u32 = 64;
const BURST_PACKETS: usize = 1000;
const BURST_BATCH: usize = 16;

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

enum Role {
    Server,
    Receiver(PathBuf),
}

fn parse_role() -> Role {
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--handoff-from" => {
                let p = args.next().expect("--handoff-from needs a path argument");
                return Role::Receiver(PathBuf::from(p));
            }
            "--role" => {
                let v = args.next().expect("--role needs a value");
                if v == "server" {
                    return Role::Server;
                } else {
                    eprintln!("unknown --role {v}, expected 'server'");
                    process::exit(2);
                }
            }
            other => {
                eprintln!("unknown argument {other}");
                process::exit(2);
            }
        }
    }
    Role::Server
}

fn main() {
    env_logger::init();
    match parse_role() {
        Role::Server => run_server(),
        Role::Receiver(path) => run_receiver(path),
    }
}

// ---------------------- server side ----------------------

fn run_server() {
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

    // Pre-bind the UDS before spawning the veth/AF_XDP setup so a
    // concurrently started receiver never loses its connect() race.
    let _ = std::fs::remove_file(HANDOFF_UDS);
    let listener = UnixListener::bind(HANDOFF_UDS).expect("bind UDS");
    println!("server listening on {HANDOFF_UDS}");

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

fn server_body(dev: (VethDevConfig, PacketGenerator), listener: UnixListener) {
    let (umem, descs) = Umem::new(
        UmemConfig::default(),
        FRAME_COUNT.try_into().unwrap(),
        false,
    )
    .expect("Umem::new");

    let sock_cfg = SocketConfig::builder()
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .build();

    let (mut tx_q, _rx_q, fq_cq) = unsafe {
        Socket::new(
            sock_cfg,
            &umem,
            &dev.0.if_name().parse().unwrap(),
            0,
        )
    }
    .expect("Socket::new");
    let (_fq, mut cq) = fq_cq.expect("fill/comp queues present");

    let socket_fd = tx_q.fd().as_raw_fd();
    let memfd = umem.memfd();
    let rx_ring_size = sock_cfg.rx_queue_size().get();
    let tx_ring_size = sock_cfg.tx_queue_size().get();

    println!(
        "server: UMEM ready ({} frames), socket_fd={socket_fd}, memfd={memfd}",
        descs.len()
    );

    // Seed our free pool with every frame and run a pre-handoff TX
    // burst. Drains completions so all frames are back in `free` by
    // the time we hand off.
    let mut free: Vec<FrameDesc> = descs;
    let (pre_sub, pre_cmp) = tx_burst(&umem, &mut tx_q, &mut cq, &mut free, BURST_PACKETS)
        .expect("server pre-handoff tx_burst");
    println!("server: pre-handoff tx_burst submitted={pre_sub} completed={pre_cmp}");
    assert_eq!(pre_sub, pre_cmp, "server: TX accounting mismatch");

    // Accept one successor and send it the bundle.
    let (mut client, addr) = listener.accept().expect("accept");
    println!("server: receiver connected from {:?}", addr);

    let payload = HandoffPayload {
        frame_count: FRAME_COUNT,
        rx_ring_size,
        tx_ring_size,
        _reserved: 0,
    };

    send_bundle(&client, &[socket_fd, memfd], &payload).expect("send_bundle");
    println!("server: bundle sent; awaiting READY");

    let mut buf = [0u8; 16];
    let n = client.read(&mut buf).expect("read READY");
    let ack = std::str::from_utf8(&buf[..n]).unwrap_or("<non-utf8>");
    println!("server: got ack {ack:?}");

    // In a real handoff we would drain TX completions, close the
    // socket/memfd, and then return. Stage 1 has no TX traffic, so we
    // just let scope-end do the cleanup.
    drop(client);
}

// ---------------------- receiver side ----------------------

fn run_receiver(path: PathBuf) {
    println!("receiver: connecting to {}", path.display());
    let mut stream = UnixStream::connect(&path).expect("connect UDS");

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
    // in `descs` is free. Run an identical burst from the receiver
    // side to prove TX still flows on the reconstructed rings.
    let mut free: Vec<FrameDesc> = descs.drain(..).collect();
    let (post_sub, post_cmp) = tx_burst(&umem, &mut tx_q, &mut cq, &mut free, BURST_PACKETS)
        .expect("receiver post-handoff tx_burst");
    println!("receiver: post-handoff tx_burst submitted={post_sub} completed={post_cmp}");
    assert_eq!(post_sub, post_cmp, "receiver: TX accounting mismatch");

    // Signal to the server that we have full possession and are happy.
    stream.write_all(b"READY").expect("send READY");
    println!("receiver: READY sent; cleaning up and exiting");

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
