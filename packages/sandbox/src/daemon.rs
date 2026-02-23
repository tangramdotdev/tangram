#[cfg(target_os = "macos")]
pub mod darwin;

#[cfg(target_os = "linux")]
pub mod linux;
