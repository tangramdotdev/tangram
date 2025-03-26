use tangram_sandbox::{Command, ExitStatus, Stdio};

#[tokio::main]
async fn main() -> std::io::Result<()> {
	#[cfg(target_os = "linux")]
	{
		let status = linux().await?;
		eprintln!("status: {status:?}");
	}

	#[cfg(target_os = "macos")]
	{
		let status = darwin().await?;
		eprintln!("status: {status:?}");
	}
	Ok(())
}

#[cfg(target_os = "linux")]
async fn linux() -> std::io::Result<ExitStatus> {
	use tangram_sandbox::Mount;

	// Create the chroot directory.
	let chroot = "/tmp/sandbox";

	// Create the command.
	let mut child = Command::new("/lib64/ld-linux-x86-64.so.2")
		.network(false)
		.mount(Mount {
			source: "/tmp".into(),
			target: "/tmp/sandbox/tmp".into(),
			fstype: Some("tmpfs".into()),
			flags: 0,
			data: None,
			readonly: false,
		})
		.mount(Mount {
			source: "/proc".into(),
			target: "/tmp/sandbox/proc".into(),
			fstype: Some("proc".into()),
			flags: 0,
			data: None,
			readonly: false,
		})
		.mount(Mount {
			source: "/tmp/sandbox/empty".into(),
			target: "/tmp/sandbox/empty".into(),
			fstype: Some("overlay".into()),
			flags: 0,
			data: Some(b"lower=/tmp/sandbox/empty,upper=/tmp/upper".to_vec()),
			readonly: false,
		})
		.stdin(Stdio::Inherit)
		.stdout(Stdio::Inherit)
		.stderr(Stdio::Inherit)
		.chroot(chroot)
		.env("PATH", "/usr/bin:/bin:/sbin:/usr/sbin:/usr/local/bin")
		.arg("/bin/sh")
		.cwd("/")
		.spawn()
		.await?;

	child.wait().await
}

#[cfg(target_os = "macos")]
async fn darwin() -> std::io::Result<ExitStatus> {
	tokio::fs::create_dir_all("/tmp/sandbox").await.ok();
	let home = std::env::var("HOME").unwrap();
	let mut child = Command::new("/bin/zsh")
		.mounts([
			("/usr", "/usr", true),
			("/bin", "/bin", true),
			("/sbin", "/sbin", true),
			("/dev", "/dev", false),
			("/tmp/sandbox", "/tmp/sandbox", false),
		])
		.mount((&home, &home, false))
		.env("PATH", "/usr/bin:/bin:/usr/local/bin:/sbin")
		.env("HOME", &home)
		.network(false)
		.stdin(Stdio::Inherit)
		.stdout(Stdio::Inherit)
		.stderr(Stdio::Inherit)
		.cwd("/tmp/sandbox")
		.spawn()
		.await?;
	child.wait().await
}
