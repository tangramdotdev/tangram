use {
	crate::Cli,
	indoc::indoc,
	std::{path::PathBuf, process::ExitCode},
	tangram_client::prelude::*,
	tangram_util::fs::Temp,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Input directory to be placed into a VM rootfs.
	#[arg(long)]
	pub source: PathBuf,

	#[arg(long)]
	pub output: PathBuf,

	/// Compression algorithm passed to mksquashfs. Default: zstd.
	#[arg(long, default_value = "zstd")]
	pub compression: String,
}

impl Cli {
	#[allow(clippy::needless_pass_by_value)]
	pub fn command_sandbox_vm_create_rootfs(args: Args) -> tg::Result<ExitCode> {
		let source = args.source.canonicalize().map_err(
			|error| tg::error!(!error, path = %args.source.display(), "failed to canonicalize the source"),
		)?;
		if !source.is_dir() {
			return Err(tg::error!(path = %source.display(), "source must be a directory"));
		}
		std::fs::remove_file(&args.output).ok();
		let current_exe = std::env::current_exe().map_err(|error| {
			tg::error!(
				!error,
				"failed to determine the running tangram binary path"
			)
		})?;
		let libexec_path = source.join("opt/tangram/libexec/tangram");
		std::fs::copy(&current_exe, &libexec_path).map_err(|error| {
			tg::error!(
				!error,
				src = %current_exe.display(),
				dst = %libexec_path.display(),
				"failed to stage the tangram binary into the rootfs",
			)
		})?;
		let temp = Temp::new().map_err(|source| tg::error!(!source, "failed to create temp"))?;
		let content = indoc!(
			"\
			/mnt d 0755 0 0
			/mnt/host d 0755 0 0
			/mnt/root d 0755 0 0
			"
		);
		std::fs::write(temp.path(), content).map_err(
			|error| tg::error!(!error, path = %temp.path().display(), "failed to write the pseudo-file definitions"),
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
			.arg(temp.path())
			.status()
			.map_err(|error| tg::error!(!error, "failed to invoke mksquashfs"))?;
		if !status.success() {
			return Err(tg::error!(exit = %status, "mksquashfs failed"));
		}
		Ok(ExitCode::SUCCESS)
	}
}
