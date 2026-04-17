//! Integration tests for `Umem::from_memfd`.
//!
//! The memfd-rebuild path does not touch AF_XDP or libxdp — it just
//! mmaps an inherited memfd and derives a fresh `Vec<FrameDesc>` from
//! the `UmemConfig` layout. That makes it cleanly testable without
//! root or network capabilities.

use std::{
    convert::TryInto,
    ffi::CString,
    io::Write,
    os::fd::{FromRawFd, OwnedFd},
    slice,
};

use xsk_rs::{
    Umem,
    config::{UmemConfig, UmemConfigBuilder, XDP_UMEM_MIN_CHUNK_SIZE},
};

const FRAME_COUNT: u32 = 8;

/// Create a sized memfd suitable for backing a UMEM of the given
/// byte length.
fn make_memfd(len: usize) -> OwnedFd {
    let name = CString::new("umem-from-memfd-test").unwrap();
    let raw = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    assert!(raw >= 0, "memfd_create failed: {}", std::io::Error::last_os_error());
    let rc = unsafe { libc::ftruncate(raw, len as libc::off_t) };
    assert_eq!(rc, 0, "ftruncate failed: {}", std::io::Error::last_os_error());
    unsafe { OwnedFd::from_raw_fd(raw) }
}

fn default_config() -> UmemConfig {
    UmemConfigBuilder::new()
        .frame_size(XDP_UMEM_MIN_CHUNK_SIZE.try_into().unwrap())
        .build()
        .unwrap()
}

#[test]
fn from_memfd_rebuilds_layout_and_descs() {
    let config = default_config();
    let frame_size = config.frame_size().get() as usize;
    let len = frame_size * FRAME_COUNT as usize;

    let memfd = make_memfd(len);
    let (_umem, descs) = Umem::from_memfd(config, FRAME_COUNT.try_into().unwrap(), memfd)
        .expect("Umem::from_memfd");

    assert_eq!(descs.len(), FRAME_COUNT as usize);

    // Frame N's data segment starts at N*frame_size + xdp_headroom + frame_headroom.
    let xdp_hr = config.xdp_headroom() as usize;
    let frame_hr = config.frame_headroom() as usize;
    for (i, d) in descs.iter().enumerate() {
        let expected = i * frame_size + xdp_hr + frame_hr;
        assert_eq!(
            d.addr(),
            expected,
            "desc[{}] addr mismatch: got {}, want {}",
            i,
            d.addr(),
            expected
        );
        assert_eq!(d.lengths().data(), 0);
        assert_eq!(d.lengths().headroom(), 0);
    }
}

#[test]
fn from_memfd_writes_are_visible_via_second_mapping() {
    // Prove the reconstructed UMEM is MAP_SHARED-backed: a second
    // independent mmap of the same memfd must observe writes issued
    // through `Umem::data_mut`.
    let config = default_config();
    let frame_size = config.frame_size().get() as usize;
    let len = frame_size * FRAME_COUNT as usize;

    let memfd = make_memfd(len);
    let memfd_raw = {
        use std::os::fd::AsRawFd as _;
        memfd.as_raw_fd()
    };

    let (umem, mut descs) = Umem::from_memfd(config, FRAME_COUNT.try_into().unwrap(), memfd)
        .expect("Umem::from_memfd");

    // Second, independent view of the same memfd.
    let shadow = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            memfd_raw,
            0,
        )
    };
    assert_ne!(
        shadow,
        libc::MAP_FAILED,
        "shadow mmap failed: {}",
        std::io::Error::last_os_error()
    );

    // Write distinct bytes into frame 0 and frame 3 via the Umem.
    let payload_a: &[u8] = b"hello";
    let payload_b: &[u8] = b"world!";
    unsafe {
        umem.data_mut(&mut descs[0])
            .cursor()
            .write_all(payload_a)
            .unwrap();
        umem.data_mut(&mut descs[3])
            .cursor()
            .write_all(payload_b)
            .unwrap();
    }

    // Read those offsets through the shadow mapping.
    let shadow_slice =
        unsafe { slice::from_raw_parts(shadow as *const u8, len) };
    let a_off = descs[0].addr();
    let b_off = descs[3].addr();
    assert_eq!(&shadow_slice[a_off..a_off + payload_a.len()], payload_a);
    assert_eq!(&shadow_slice[b_off..b_off + payload_b.len()], payload_b);

    // Drop shadow mapping before dropping the Umem (Umem owns the
    // primary mmap; shadow is independent).
    unsafe {
        libc::munmap(shadow, len);
    }

    drop(umem);
}

#[test]
fn from_memfd_undersized_fd_fails_cleanly() {
    // memfd sized for 2 frames but caller asks for 8 — mmap of the
    // full region must fail, not silently corrupt memory.
    let config = default_config();
    let frame_size = config.frame_size().get() as usize;
    let short_len = frame_size * 2;
    let memfd = make_memfd(short_len);

    let err = Umem::from_memfd(config, FRAME_COUNT.try_into().unwrap(), memfd)
        .expect_err("undersized memfd must not reconstruct a UMEM");
    // The exact errno is kernel-dependent; just assert the source is
    // an io::Error rather than asserting a specific kind.
    use std::error::Error as _;
    assert!(
        err.source().is_some(),
        "UmemCreateError should wrap the underlying io::Error"
    );
}
