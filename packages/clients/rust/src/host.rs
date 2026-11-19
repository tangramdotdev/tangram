/// The hosts that Tangram supports.
pub const SUPPORTED: [&str; 4] = [
	"aarch64-darwin",
	"aarch64-linux",
	"x86_64-darwin",
	"x86_64-linux",
];

/// Get the current host.
#[must_use]
pub fn current() -> &'static str {
	#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
	{
		"aarch64-darwin"
	}
	#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
	{
		"aarch64-linux"
	}
	#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
	{
		"x86_64-darwin"
	}
	#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
	{
		"x86_64-linux"
	}
}
