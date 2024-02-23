use std::sync::atomic::AtomicU32;
use std::time::Duration;

#[cfg(target_os = "linux")]
#[inline(always)]
pub fn wait(futex: &AtomicU32, value: u32, timeout: Duration) {
    let timeout = libc::timespec {
        tv_sec: i64::try_from(timeout.as_secs()).expect("Timeout is overflow."),
        tv_nsec: timeout.subsec_nanos().into(),
    };
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex.as_ptr(),
            libc::FUTEX_WAIT,
            value,
            &timeout,
        );
    }
}

#[cfg(target_os = "linux")]
#[inline(always)]
pub fn wake(futex: &AtomicU32) {
    unsafe {
        libc::syscall(libc::SYS_futex, futex.as_ptr(), libc::FUTEX_WAKE, i32::MAX);
    }
}

#[cfg(target_os = "macos")]
#[inline(always)]
pub fn wait(futex: &AtomicU32, value: u32, timeout: Duration) {
    let timeout = u32::try_from(timeout.as_millis()).expect("Timeout is overflow.");
    unsafe {
        // https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/sys_ulock.c#L531
        ulock_sys::__ulock_wait(
            ulock_sys::darwin19::UL_COMPARE_AND_WAIT_SHARED,
            futex.as_ptr().cast(),
            value as _,
            timeout,
        );
    }
}

#[cfg(target_os = "macos")]
#[inline(always)]
pub fn wake(futex: &AtomicU32) {
    unsafe {
        ulock_sys::__ulock_wake(
            ulock_sys::darwin19::UL_COMPARE_AND_WAIT_SHARED,
            futex.as_ptr().cast(),
            0,
        );
    }
}

#[cfg(target_os = "freebsd")]
#[inline(always)]
pub fn wait(futex: &AtomicU32, value: u32, timeout: Duration) {
    let ptr: *const AtomicU32 = futex;
    let mut timeout = libc::timespec {
        tv_sec: i64::try_from(timeout.as_secs()).expect("Timeout is overflow."),
        tv_nsec: timeout.subsec_nanos().into(),
    };
    unsafe {
        // https://github.com/freebsd/freebsd-src/blob/main/sys/kern/kern_umtx.c#L3943
        // https://github.com/freebsd/freebsd-src/blob/main/sys/kern/kern_umtx.c#L3836
        libc::_umtx_op(
            ptr as *mut libc::c_void,
            libc::UMTX_OP_WAIT_UINT,
            value as libc::c_ulong,
            std::mem::size_of_val(&timeout) as *mut std::ffi::c_void,
            std::ptr::addr_of_mut!(timeout).cast(),
        );
    };
}

#[cfg(target_os = "freebsd")]
#[inline(always)]
pub fn wake(futex: &AtomicU32) {
    let ptr: *const AtomicU32 = futex;
    unsafe {
        libc::_umtx_op(
            ptr as *mut libc::c_void,
            libc::UMTX_OP_WAKE,
            i32::MAX as libc::c_ulong,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        );
    };
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
compile_error!("Target is not supported.");
