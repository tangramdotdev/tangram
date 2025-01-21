#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
    pub signal: i32,
}

// Define a subset of common signals from POSIX using ARM/x86 convention.
// https://man7.org/linux/man-pages/man7/signal.7.html
pub const SIGINT: i32 = 2;
pub const SIGKILL: i32 = 9;
pub const SIGUSR1: i32 = 10;
pub const SIGUSR2: i32 = 11;
pub const SIGALRM: i32 = 14;
pub const SIGTERM: i32 = 15;
pub const SIGWINCH: i32 = 28;
