// The hosts that do not name a machine.
const VIRTUAL_HOSTS: [&str; 2] = ["builtin", "js"];

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

/// Determine whether a host is virtual, meaning that it does not name a machine, so any runner can run its processes.
#[must_use]
pub fn is_virtual(host: &str) -> bool {
	VIRTUAL_HOSTS.contains(&host)
}
