#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_arch = "x86_64")]
pub mod x86_64;

pub fn initialize() {
    #[cfg(target_os = "linux")]
    {
        self::linux::ctor_memfd();
    }
    #[cfg(target_arch = "x86_64")]
    {
        self::x86_64::ctor_avx512fp16();
        self::x86_64::ctor_avx512vpopcntdq();
        self::x86_64::ctor_v2();
        self::x86_64::ctor_v3();
        self::x86_64::ctor_v4();
    }
}
