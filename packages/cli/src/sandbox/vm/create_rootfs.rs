use {
	crate::Cli,
	std::{path::PathBuf, process::ExitCode},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub source: PathBuf,

	#[arg(long)]
	pub output: PathBuf,

	/// Compression algorithm passed to mksquashfs. Default: zstd.
	#[arg(long, default_value = "zstd")]
	pub compression: String,
}

impl Cli {
	pub fn command_sandbox_vm_create_rootfs(args: Args) -> tg::Result<ExitCode> {
		let source = args
			.source
			.canonicalize()
			.map_err(|error| tg::error!(!error, path = %args.source.display(), "failed to canonicalize the source"))?;
		if !source.is_dir() {
			return Err(tg::error!(path = %source.display(), "source must be a directory"));
		}

		std::fs::remove_file(&args.output).ok();

		// Stage the running tangram binary into the rootfs's libexec slot so
		// the bin/tangram wrapper has something to exec.
		let current_exe = std::env::current_exe().map_err(
			|error| tg::error!(!error, "failed to determine the running tangram binary path"),
		)?;
		let libexec_path = source.join("opt/tangram/libexec/tangram");
		std::fs::copy(&current_exe, &libexec_path).map_err(|error| {
			tg::error!(
				!error,
				src = %current_exe.display(),
				dst = %libexec_path.display(),
				"failed to stage the tangram binary into the rootfs",
			)
		})?;

		// Pre-create placeholders for the per-sandbox bind-mount targets so
		// init can bind-mount over them at runtime.
		let pseudo_path = std::env::temp_dir()
			.join(format!("tangram-rootfs-pseudo-{}.txt", std::process::id()));
		let pseudo_content = "\
/etc/init.json f 0644 0 0 true
/opt/tangram/artifacts d 0755 0 0
/opt/tangram/output d 0755 0 0
/run d 0755 0 0
/run/sandbox d 0755 0 0
";
		std::fs::write(&pseudo_path, pseudo_content).map_err(
			|error| tg::error!(!error, path = %pseudo_path.display(), "failed to write the pseudo-file definitions"),
		)?;

		let status = std::process::Command::new("mksquashfs")
			.arg(&source)
			.arg(&args.output)
			.arg("-comp")
			.arg(&args.compression)
			.arg("-all-root")
			.arg("-noappend")
			.arg("-quiet")
			.arg("-pf")
			.arg(&pseudo_path)
			.status()
			.map_err(|error| tg::error!(!error, "failed to invoke mksquashfs"))?;

		std::fs::remove_file(&pseudo_path).ok();

		if !status.success() {
			return Err(tg::error!(exit = %status, "mksquashfs failed"));
		}

		eprintln!("created rootfs image at {}", args.output.display());
		Ok(ExitCode::SUCCESS)
	}
}
