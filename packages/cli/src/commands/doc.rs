use super::PackageArgs;
use crate::Cli;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tangram_language::ROOT_MODULE_FILE_NAME;

/// Generate documentation.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	#[command(flatten)]
	pub package_args: PackageArgs,

	/// Generate the documentation for the runtime.
	#[arg(short, long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_doc(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Create the language server.
		let server = tangram_language::Server::new(tg, tokio::runtime::Handle::current());

		let (module, path) = if args.runtime {
			// Create the module.
			let module = tangram_language::Module::Library(tangram_language::module::Library {
				path: "tangram.d.ts".parse().unwrap(),
			});
			(module, "tangram.d.ts")
		} else {
			// Create the package.
			let (package, lock) = tg::package::get_with_lock(tg, &args.package).await?;

			// Create the module.
			let module = tangram_language::Module::Normal(tangram_language::module::Normal {
				package: package.id(tg).await?.clone(),
				path: ROOT_MODULE_FILE_NAME.parse().unwrap(),
				lock: lock.id(tg).await?.clone(),
			});

			(module, ROOT_MODULE_FILE_NAME)
		};

		// Get the docs.
		let docs = server.docs(&module).await?;
		// Render the docs to JSON.
		let docs = serde_json::to_string_pretty(&serde_json::json!({
			path.to_owned(): docs,
		}))
		.wrap_err("Failed to serialize the docs.")?;

		// Print the docs.
		println!("{docs}");

		Ok(())
	}
}
