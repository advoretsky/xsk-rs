pub use inner::Mmap;

use std::{io, ptr::NonNull};

#[cfg(not(test))]
mod inner {
    use libc::{MAP_FAILED, MAP_POPULATE, MAP_SHARED, PROT_READ, PROT_WRITE};
    use log::error;
    use std::{
        ffi::CStr,
        os::fd::{AsFd, BorrowedFd, FromRawFd, OwnedFd, RawFd},
        ptr,
    };

    use super::*;

    // Backing the UMEM with a memfd (instead of MAP_ANONYMOUS) makes the
    // memory region shareable across processes via SCM_RIGHTS, which is
    // needed for zero-downtime re-exec handoff. The file descriptor is
    // kept alive for the lifetime of the mapping.
    #[derive(Debug)]
    pub struct Mmap {
        addr: NonNull<libc::c_void>,
        len: usize,
        fd: OwnedFd,
    }

    unsafe impl Send for Mmap {}

    impl Mmap {
        pub fn new(len: usize, use_huge_pages: bool) -> io::Result<Self> {
            // MFD_CLOEXEC: don't leak FD across exec unless explicitly handed off.
            // MFD_HUGETLB: back memfd with huge pages when requested.
            let name = CStr::from_bytes_with_nul(b"xsk-rs-umem\0").unwrap();
            let mut memfd_flags = libc::MFD_CLOEXEC;
            if use_huge_pages {
                memfd_flags |= libc::MFD_HUGETLB;
            }

            let raw_fd = unsafe { libc::memfd_create(name.as_ptr(), memfd_flags as libc::c_uint) };
            if raw_fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

            if !use_huge_pages {
                // Huge-page memfds refuse ftruncate (size is set at creation
                // by the allocation itself); regular memfds need explicit sizing.
                let rc = unsafe { libc::ftruncate(raw_fd, len as libc::off_t) };
                if rc != 0 {
                    return Err(io::Error::last_os_error());
                }
            }

            Self::from_fd(fd, len)
        }

        /// Map an existing memfd into this process. The fd is adopted
        /// and closed when the returned `Mmap` is dropped.
        pub fn from_fd(fd: OwnedFd, len: usize) -> io::Result<Self> {
            use std::os::fd::AsRawFd;

            // MAP_SHARED: kernel-side UMEM registration sees the same
            // physical pages as userspace.
            // MAP_POPULATE: pre-populate page tables.
            let flags = MAP_SHARED | MAP_POPULATE;

            let addr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    len,
                    PROT_READ | PROT_WRITE,
                    flags,
                    fd.as_raw_fd(),
                    0,
                )
            };

            if addr == MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            let addr = NonNull::new(addr).expect("non-null after successful mmap");
            Ok(Mmap { addr, len, fd })
        }

        /// Returns a pointer to the start of the mmap'd region.
        #[inline]
        pub fn addr(&self) -> NonNull<libc::c_void> {
            self.addr
        }

        /// Borrow the memfd that backs this region, e.g. for SCM_RIGHTS.
        #[inline]
        pub fn as_fd(&self) -> BorrowedFd<'_> {
            self.fd.as_fd()
        }

        /// Raw memfd descriptor. Caller must not close it.
        #[inline]
        pub fn as_raw_fd(&self) -> RawFd {
            use std::os::fd::AsRawFd;
            self.fd.as_raw_fd()
        }
    }

    impl Drop for Mmap {
        fn drop(&mut self) {
            let err = unsafe { libc::munmap(self.addr.as_ptr(), self.len) };

            if err != 0 {
                error!(
                    "`munmap()` failed with error: {}",
                    io::Error::last_os_error()
                );
            }
        }
    }
}

#[cfg(test)]
mod inner {
    use std::mem::ManuallyDrop;

    use super::*;

    #[derive(Debug)]
    struct VecParts<T> {
        ptr: NonNull<T>,
        len: usize,
        capacity: usize,
    }

    unsafe impl<T> Send for VecParts<T> {}

    impl<T> VecParts<T> {
        fn new(v: Vec<T>) -> Self {
            let mut v = ManuallyDrop::new(v);

            Self {
                ptr: NonNull::new(v.as_mut_ptr()).expect("obtained pointer from Vec"),
                len: v.len(),
                capacity: v.capacity(),
            }
        }
    }

    impl<T> Drop for VecParts<T> {
        fn drop(&mut self) {
            unsafe { Vec::from_raw_parts(self.ptr.as_ptr(), self.len, self.capacity) };
        }
    }

    /// A mocked [`Mmap`] that uses the heap for memory.
    #[derive(Debug)]
    pub struct Mmap(VecParts<u8>);

    impl Mmap {
        pub fn new(len: usize, _use_huge_pages: bool) -> io::Result<Self> {
            Ok(Self(VecParts::new(vec![0; len])))
        }

        /// Returns a pointer to the start of the mmap'd region.
        #[inline]
        pub fn addr(&self) -> NonNull<libc::c_void> {
            NonNull::new(self.0.ptr.as_ptr() as *mut libc::c_void).unwrap()
        }

        /// Heap-backed mock has no real fd; return -1 to keep the
        /// call-site shape identical to the real [`Mmap`].
        #[inline]
        pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
            -1
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn confirm_pointer_offset_is_a_single_byte() {
        assert_eq!(std::mem::size_of::<libc::c_void>(), 1);
    }
}
