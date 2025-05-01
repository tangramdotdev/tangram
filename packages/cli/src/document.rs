use crate::{Cli, util::infer_module_kind};
use tangram_client::{self as tg, prelude::*};
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
	pub async fn command_document(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get the module.
		let module = if args.runtime {
			tg::module::Data {
				kind: tg::module::Kind::Dts,
				referent: tg::Referent {
					item: tg::module::data::Item::Path("tangram.d.ts".into()),
					path: None,
					subpath: None,
					tag: None,
				},
			}
		} else {
			let referent = self.get_reference(&args.reference).await?;
			let Either::Right(object) = referent.item else {
				return Err(tg::error!("expected an object"));
			};
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
			let root_module_file_name =
				tg::package::try_get_root_module_file_name(&handle, Either::Left(&referent.item))
					.await?
					.ok_or_else(|| tg::error!("expected a root module"))?;
			let kind = if let Some(kind) = infer_module_kind(root_module_file_name) {
				kind
			} else {
				match referent
					.item
					.unwrap_directory_ref()
					.get(&handle, root_module_file_name)
					.await?
				{
					tg::Artifact::Directory(_) => tg::module::Kind::Directory,
					tg::Artifact::File(_) => tg::module::Kind::File,
					tg::Artifact::Symlink(_) => tg::module::Kind::Symlink,
				}
			};
			let item = tg::module::data::Item::Object(referent.item.id(&handle).await?);
			let path = referent.path;
			let subpath = Some(referent.subpath.map_or_else(
				|| root_module_file_name.into(),
				|path| path.join(root_module_file_name),
			));
			let tag = referent.tag;
			let referent = tg::Referent {
				item,
				path,
				subpath,
				tag,
			};
			tg::module::Data { referent, kind }
		};

		// Document the module.
		let arg = tg::document::Arg { module, remote };
		let output = handle.document(arg).await?;

		// Print the output.
		Self::output_json(&output, args.pretty).await?;

		Ok(())
	}
}
