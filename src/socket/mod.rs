//! Types for creating and using an AF_XDP [`Socket`].

mod fd;
pub use fd::{Fd, XdpStatistics};

mod rx_queue;
pub use rx_queue::RxQueue;

mod tx_queue;
pub use tx_queue::TxQueue;

use libxdp_sys::xsk_socket;
use std::{
    borrow::Borrow,
    error::Error,
    fmt, io,
    os::fd::{AsRawFd as _, OwnedFd},
    ptr::{self, NonNull},
    sync::{Arc, Mutex},
};

use crate::{
    config::{Interface, SocketConfig},
    ring::{XskRingCons, XskRingProd},
    umem::{CompQueue, FillQueue, Umem},
};

/// Wrapper around a pointer to some AF_XDP socket.
#[derive(Debug)]
struct XskSocket(NonNull<xsk_socket>);

impl XskSocket {
    /// # Safety
    ///
    /// Only one instance of this struct may exist since it deletes
    /// the socket as part of its [`Drop`] impl. If there are copies or
    /// clones of `ptr` then care must be taken to ensure they aren't
    /// used once this struct goes out of scope, and that they don't
    /// delete the socket themselves.
    unsafe fn new(ptr: NonNull<xsk_socket>) -> Self {
        Self(ptr)
    }
}

impl Drop for XskSocket {
    fn drop(&mut self) {
        // SAFETY: unsafe constructor contract guarantees that the
        // socket has not been deleted already.
        unsafe {
            libxdp_sys::xsk_socket__delete(self.0.as_mut());
        }
    }
}

unsafe impl Send for XskSocket {}

// A socket created via libxdp closes its FD via `xsk_socket__delete`.
// A socket reconstructed from an inherited FD has no libxdp handle, so
// we own the FD directly and let `OwnedFd::drop` close it.
#[derive(Debug)]
enum SocketOwner {
    Libxdp(XskSocket),
    Inherited(OwnedFd),
}

/// Tracks a raw `mmap` region we own ourselves (Path A reconstruction).
#[derive(Debug)]
struct RawMmap {
    addr: *mut libc::c_void,
    len: usize,
}

unsafe impl Send for RawMmap {}

impl Drop for RawMmap {
    fn drop(&mut self) {
        let err = unsafe { libc::munmap(self.addr, self.len) };
        if err != 0 {
            // Matches xsk-rs's existing failure-logging style elsewhere.
            log::error!(
                "munmap of ring region failed: {}",
                io::Error::last_os_error()
            );
        }
    }
}

#[derive(Debug)]
struct SocketInner {
    // `owner` must appear before `umem` to ensure correct drop order.
    owner: SocketOwner,
    _umem: Umem,
    // RX/TX ring mmap regions when `owner` is `Inherited`. Empty for
    // libxdp-created sockets (xsk_socket__delete unmaps those).
    // Dropped after `owner` closes the FD (struct field order +
    // Vec<RawMmap> Drop).
    rx_tx_mmaps: Vec<RawMmap>,
}

impl SocketInner {
    fn new_libxdp(ptr: XskSocket, umem: Umem) -> Self {
        Self {
            owner: SocketOwner::Libxdp(ptr),
            _umem: umem,
            rx_tx_mmaps: Vec::new(),
        }
    }

    fn new_inherited(fd: OwnedFd, umem: Umem) -> Self {
        Self {
            owner: SocketOwner::Inherited(fd),
            _umem: umem,
            rx_tx_mmaps: Vec::new(),
        }
    }
}

/// An AF_XDP socket.
///
/// More details can be found in the
/// [docs](https://www.kernel.org/doc/html/latest/networking/af_xdp.html)
#[derive(Debug)]
pub struct Socket {
    fd: Fd,
    _inner: Arc<Mutex<SocketInner>>,
}

impl Socket {
    /// Create and bind a new AF_XDP socket to a given interface and
    /// queue id using the underlying UMEM.
    ///
    /// May require root permissions to create successfully.
    ///
    /// Whether you can expect the returned `Option<(FillQueue,
    /// CompQueue)>` to be [`Some`] or [`None`] depends on a couple of
    /// things:
    ///
    ///  1. If the [`Umem`] is currently shared (i.e. being used for
    ///  >=1 AF_XDP sockets elsewhere):
    ///
    ///    - If the `(if_name, queue_id)` pair is not bound to, expect
    ///    [`Some`].
    ///
    ///    - If the `(if_name, queue_id)` pair is bound to, expect
    ///    [`None`] and use the [`FillQueue`] and [`CompQueue`]
    ///    originally returned for this pair.
    ///
    ///  2. If the [`Umem`] is not currently shared, expect [`Some`].
    ///
    /// For further details on using a shared [`Umem`] please see the
    /// [docs](https://www.kernel.org/doc/html/latest/networking/af_xdp.html#xdp-shared-umem-bind-flag).
    ///
    /// # Safety
    ///
    /// If sharing the [`Umem`] and the `(if_name, queue_id)` pair is
    /// already bound to, then the
    /// [`XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD`] flag must be
    /// set. Otherwise, a double-free may occur when dropping sockets
    /// if the program has already been detached.
    ///
    /// [`XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD`]: crate::config::LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD
    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::type_complexity)]
    pub unsafe fn new(
        config: SocketConfig,
        umem: &Umem,
        if_name: &Interface,
        queue_id: u32,
    ) -> Result<(TxQueue, RxQueue, Option<(FillQueue, CompQueue)>), SocketCreateError> {
        let mut socket_ptr = ptr::null_mut();
        let mut tx_q = XskRingProd::default();
        let mut rx_q = XskRingCons::default();

        let (err, fq, cq) = unsafe {
            umem.with_ptr_and_saved_queues(|xsk_umem, saved_fq_and_cq| {
                let (mut fq, mut cq) = saved_fq_and_cq
                    .take()
                    .unwrap_or_else(|| (Box::default(), Box::default()));

                let err = libxdp_sys::xsk_socket__create_shared(
                    &mut socket_ptr,
                    if_name.as_cstr().as_ptr(),
                    queue_id,
                    xsk_umem,
                    rx_q.as_mut(),
                    tx_q.as_mut(),
                    fq.as_mut().as_mut(), // double deref due to Box
                    cq.as_mut().as_mut(),
                    &config.into(),
                );

                (err, fq, cq)
            })
        };

        if err != 0 {
            return Err(SocketCreateError {
                reason: "non-zero error code returned when creating AF_XDP socket",
                err: io::Error::from_raw_os_error(-err),
            });
        }

        let socket_ptr = match NonNull::new(socket_ptr) {
            Some(init_xsk) => {
                // SAFETY: this is the only `XskSocket` instance for
                // this pointer, and no other pointers to the socket
                // exist.
                unsafe { XskSocket::new(init_xsk) }
            }
            None => {
                return Err(SocketCreateError {
                    reason: "returned socket pointer was null",
                    err: io::Error::from_raw_os_error(-err),
                });
            }
        };

        let fd = unsafe { libxdp_sys::xsk_socket__fd(socket_ptr.0.as_ref()) };

        if fd < 0 {
            return Err(SocketCreateError {
                reason: "failed to retrieve AF_XDP socket file descriptor",
                err: io::Error::from_raw_os_error(-fd),
            });
        }

        let socket = Socket {
            fd: Fd::new(fd),
            _inner: Arc::new(Mutex::new(SocketInner::new_libxdp(socket_ptr, umem.clone()))),
        };

        let tx_q = if tx_q.is_ring_null() {
            return Err(SocketCreateError {
                reason: "returned tx queue ring is null",
                err: io::Error::from_raw_os_error(-err),
            });
        } else {
            TxQueue::new(tx_q, socket.clone())
        };

        let rx_q = if rx_q.is_ring_null() {
            return Err(SocketCreateError {
                reason: "returned rx queue ring is null",
                err: io::Error::from_raw_os_error(-err),
            });
        } else {
            RxQueue::new(rx_q, socket)
        };

        let fq_and_cq = match (fq.is_ring_null(), cq.is_ring_null()) {
            (true, true) => None,
            (false, false) => {
                let fq = FillQueue::new(*fq, umem.clone());
                let cq = CompQueue::new(*cq, umem.clone());

                Some((fq, cq))
            }
            _ => {
                return Err(SocketCreateError {
                    reason: "fill queue xor comp queue ring is null, either both or neither should be non-null",
                    err: io::Error::from_raw_os_error(-err),
                });
            }
        };

        Ok((tx_q, rx_q, fq_and_cq))
    }

    /// Reconstruct an AF_XDP socket in a successor process from an
    /// inherited data-socket FD. Bypasses libxdp entirely: queries
    /// `XDP_MMAP_OFFSETS` and `mmap`s the RX/TX/Fill/Completion rings
    /// directly, then populates `XskRingProd`/`XskRingCons` from the
    /// returned offsets.
    ///
    /// `umem` must be the [`Umem`] reconstructed via
    /// [`Umem::from_memfd`] against the same memfd that the original
    /// socket registered. `config` must reflect the ring sizes used at
    /// original socket creation — the kernel retains those; passing a
    /// different value would produce ring wrappers that miscount
    /// entries.
    ///
    /// See `crouter/docs/xdp_reexec_investigation.md` for the
    /// lifetime and FD-inheritance model.
    ///
    /// # Safety
    ///
    /// `fd` must be an AF_XDP socket FD inherited from a process that
    /// still held the kernel binding at the time of the exec
    /// (typically via [`libc::execv`] without closing the FD and after
    /// clearing `FD_CLOEXEC`). The caller must not concurrently use
    /// `fd` in any other AF_XDP wrapper.
    #[cfg(not(test))]
    pub unsafe fn from_raw_fd(
        config: SocketConfig,
        umem: &Umem,
        fd: OwnedFd,
    ) -> Result<(TxQueue, RxQueue, Option<(FillQueue, CompQueue)>), SocketCreateError> {
        use crate::umem::{CompQueue, FillQueue};

        let offsets = getsockopt_mmap_offsets(fd.as_raw_fd()).map_err(|e| SocketCreateError {
            reason: "getsockopt(XDP_MMAP_OFFSETS) failed on inherited FD",
            err: e,
        })?;

        let rx_size = config.rx_queue_size().get();
        let tx_size = config.tx_queue_size().get();
        // The Fill/Completion ring sizes were set on the ORIGINAL
        // `Umem::new` path — they are not part of `SocketConfig`.
        // libxdp defaults both to XSK_RING_*__DEFAULT_NUM_DESCS so we
        // match that here. A future API iteration could take explicit
        // sizes; for the prototype we assume defaults.
        let fr_size = libxdp_sys::XSK_RING_PROD__DEFAULT_NUM_DESCS;
        let cr_size = libxdp_sys::XSK_RING_CONS__DEFAULT_NUM_DESCS;

        let xdp_desc_sz = std::mem::size_of::<libxdp_sys::xdp_desc>();
        let u64_sz = std::mem::size_of::<u64>();

        // RX: consumer ring (kernel produces, we consume).
        let rx_len = (offsets.rx.desc + (rx_size as u64) * xdp_desc_sz as u64) as usize;
        let rx_addr = mmap_ring(&fd, rx_len, libxdp_sys::XDP_PGOFF_RX_RING.into())?;
        let rx = build_ring_cons(rx_addr, &offsets.rx, rx_size);

        // TX: producer ring (we produce, kernel consumes).
        let tx_len = (offsets.tx.desc + (tx_size as u64) * xdp_desc_sz as u64) as usize;
        let tx_addr = mmap_ring(&fd, tx_len, libxdp_sys::XDP_PGOFF_TX_RING.into())?;
        let tx = build_ring_prod(tx_addr, &offsets.tx, tx_size);

        // Fill: producer ring (we produce, kernel consumes UMEM frames).
        let fr_len = (offsets.fr.desc + (fr_size as u64) * u64_sz as u64) as usize;
        let fr_addr = mmap_ring(&fd, fr_len, libxdp_sys::XDP_UMEM_PGOFF_FILL_RING as i64)?;
        let fr = build_ring_prod(fr_addr, &offsets.fr, fr_size);

        // Completion: consumer ring (kernel produces, we consume).
        let cr_len = (offsets.cr.desc + (cr_size as u64) * u64_sz as u64) as usize;
        let cr_addr = mmap_ring(&fd, cr_len, libxdp_sys::XDP_UMEM_PGOFF_COMPLETION_RING as i64)?;
        let cr = build_ring_cons(cr_addr, &offsets.cr, cr_size);

        let raw_fd = fd.as_raw_fd();

        // Transfer Fill/Completion ring regions to the Umem so they
        // outlive any FillQueue/CompQueue holding pointers into them.
        umem.inner_attach_restored_fc(fr_addr, fr_len);
        umem.inner_attach_restored_fc(cr_addr, cr_len);

        // RX/TX live on the Socket — dropped with SocketInner when
        // all clones of Socket are gone.
        let socket = Socket {
            fd: Fd::new(raw_fd),
            _inner: Arc::new(Mutex::new(SocketInner::new_inherited(fd, umem.clone()))),
        };

        let tx_q = TxQueue::new(tx, socket.clone());
        let rx_q = RxQueue::new(rx, socket.clone());

        // Stash the RX/TX mmap regions on the Socket so they live as
        // long as any Queue clone of this Socket.
        socket.attach_rx_tx_mmaps(rx_addr, rx_len, tx_addr, tx_len);

        let fq = FillQueue::new(fr, umem.clone());
        let cq = CompQueue::new(cr, umem.clone());

        Ok((tx_q, rx_q, Some((fq, cq))))
    }

    #[cfg(not(test))]
    fn attach_rx_tx_mmaps(
        &self,
        rx_addr: *mut libc::c_void,
        rx_len: usize,
        tx_addr: *mut libc::c_void,
        tx_len: usize,
    ) {
        let mut inner = self._inner.lock().unwrap();
        inner.rx_tx_mmaps.push(RawMmap {
            addr: rx_addr,
            len: rx_len,
        });
        inner.rx_tx_mmaps.push(RawMmap {
            addr: tx_addr,
            len: tx_len,
        });
    }
}

// ---- Raw AF_XDP ring reconstruction helpers (Path A: no libxdp) ----

const SOL_XDP: libc::c_int = 283;

#[cfg(not(test))]
fn getsockopt_mmap_offsets(
    fd: std::os::fd::RawFd,
) -> io::Result<libxdp_sys::xdp_mmap_offsets> {
    use std::mem::MaybeUninit;
    let mut off = MaybeUninit::<libxdp_sys::xdp_mmap_offsets>::zeroed();
    let mut len = std::mem::size_of::<libxdp_sys::xdp_mmap_offsets>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            SOL_XDP,
            libxdp_sys::XDP_MMAP_OFFSETS as libc::c_int,
            off.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if rc != 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { off.assume_init() })
    }
}

#[cfg(not(test))]
fn mmap_ring(
    fd: &OwnedFd,
    len: usize,
    offset: i64,
) -> Result<*mut libc::c_void, SocketCreateError> {
    let addr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd.as_raw_fd(),
            offset as libc::off_t,
        )
    };
    if addr == libc::MAP_FAILED {
        Err(SocketCreateError {
            reason: "mmap of inherited ring failed",
            err: io::Error::last_os_error(),
        })
    } else {
        Ok(addr)
    }
}

#[cfg(not(test))]
fn build_ring_prod(
    base: *mut libc::c_void,
    off: &libxdp_sys::xdp_ring_offset,
    size: u32,
) -> XskRingProd {
    let mut r = XskRingProd::default();
    let m = r.as_mut();
    m.mask = size - 1;
    m.size = size;
    unsafe {
        m.producer = (base as *mut u8).add(off.producer as usize) as *mut u32;
        m.consumer = (base as *mut u8).add(off.consumer as usize) as *mut u32;
        m.flags = (base as *mut u8).add(off.flags as usize) as *mut u32;
        m.ring = (base as *mut u8).add(off.desc as usize) as *mut std::ffi::c_void;
        m.cached_prod = *m.producer;
        // For producer rings (Tx, Fill) libxdp stores cached_cons as
        // *consumer + ring_size so nb_free math wraps correctly.
        m.cached_cons = (*m.consumer).wrapping_add(size);
    }
    r
}

#[cfg(not(test))]
fn build_ring_cons(
    base: *mut libc::c_void,
    off: &libxdp_sys::xdp_ring_offset,
    size: u32,
) -> XskRingCons {
    let mut r = XskRingCons::default();
    let m = r.as_mut();
    m.mask = size - 1;
    m.size = size;
    unsafe {
        m.producer = (base as *mut u8).add(off.producer as usize) as *mut u32;
        m.consumer = (base as *mut u8).add(off.consumer as usize) as *mut u32;
        m.flags = (base as *mut u8).add(off.flags as usize) as *mut u32;
        m.ring = (base as *mut u8).add(off.desc as usize) as *mut std::ffi::c_void;
        m.cached_prod = *m.producer;
        m.cached_cons = *m.consumer;
    }
    r
}

impl Clone for Socket {
    fn clone(&self) -> Self {
        Self {
            fd: self.fd.clone(),
            _inner: self._inner.clone(),
        }
    }
}

/// Error detailing why [`Socket`] creation failed.
#[derive(Debug)]
pub struct SocketCreateError {
    reason: &'static str,
    err: io::Error,
}

impl fmt::Display for SocketCreateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl Error for SocketCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.err.borrow())
    }
}

impl Socket {
    /// Update XSKMAP with this socket using the proper libxdp function.
    /// This is the correct way to register AF_XDP sockets in XSKMAP.
    pub fn update_xskmap(&self, map_fd: i32) -> Result<(), Box<dyn Error>> {
        let inner = self._inner.lock().map_err(|e| format!("Failed to lock socket: {}", e))?;
        let xsk_ptr = match &inner.owner {
            SocketOwner::Libxdp(p) => p.0.as_ptr(),
            SocketOwner::Inherited(_) => {
                return Err("update_xskmap is not available on sockets reconstructed via from_raw_fd; \
                            the successor must update the XSKMAP by raw FD instead"
                    .into());
            }
        };
        
        unsafe {
            let ret = libxdp_sys::xsk_socket__update_xskmap(xsk_ptr, map_fd);
            if ret < 0 {
                return Err(format!("xsk_socket__update_xskmap failed: {}", ret).into());
            }
        }
        
        Ok(())
    }
}
