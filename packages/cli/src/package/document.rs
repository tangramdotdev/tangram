use super::infer_module_kind;
use crate::Cli;
use tangram_client::{self as tg, Handle as _};
use tangram_either::Either;

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// Generate the documentation for the runtime.
	#[arg(long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_package_document(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Handle the runtime doc.
		if args.runtime {
			let output = handle.get_js_runtime_doc().await?;
			Self::output_json(&output, args.pretty).await?;
			return Ok(());
		}

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let Either::Right(object) = referent.item else {
			return Err(tg::error!("expected an object"));
		};

		// Resolve the referent.
		let referent = if let Some(subpath) = &referent.subpath {
			let directory = object
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			let item = directory.get(&handle, subpath).await?.into();
			let path = referent.path.map(|path| path.join(subpath));
			tg::Referent {
				item,
				path,
				subpath: None,
				tag: referent.tag,
			}
		} else {
			tg::Referent {
				item: object,
				path: referent.path,
				subpath: referent.subpath,
				tag: referent.tag,
			}
		};

		// Get the root module.
		let root_module =
			tg::package::try_get_root_module_file_name(&handle, Either::Left(&referent.item))
				.await?
				.ok_or_else(|| tg::error!("expected a root module"))?;
		let kind = if let Some(kind) = infer_module_kind(root_module) {
			kind
		} else {
			match referent
				.item
				.unwrap_directory_ref()
				.get(&handle, root_module)
				.await?
			{
				tg::Artifact::Directory(_) => tg::module::Kind::Directory,
				tg::Artifact::File(_) => tg::module::Kind::File,
				tg::Artifact::Symlink(_) => tg::module::Kind::Symlink,
			}
		};

		// Create the module.
		let referent = tg::Referent {
			item: tg::module::Item::Object(referent.item.id(&handle).await?),
			path: referent.path,
			subpath: Some(
				referent
					.subpath
					.map_or_else(|| root_module.into(), |path| path.join(root_module)),
			),
			tag: referent.tag,
		};
		let package = tg::Module { referent, kind };

		// Document the module.
		let arg = tg::package::document::Arg { package, remote };
		let output = handle.document_package(arg).await?;

		// Print the output.
		Self::output_json(&output, args.pretty).await?;

		Ok(())
	}
}
