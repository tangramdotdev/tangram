use {std::path::PathBuf, tangram_client::prelude::*, tangram_uri::Uri};

#[derive(Clone, Debug)]
pub struct Arg {
	pub library_paths: Vec<PathBuf>,
	pub path: PathBuf,
	pub tangram_path: PathBuf,
	pub url: Uri,
}

pub fn init(arg: &Arg) -> tg::Result<()> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|source| tg::error!(!source, "failed to create the runtime"))?;

	runtime.block_on(async move {
		let server = crate::server::Server::new(crate::server::Arg {
			library_paths: arg.library_paths.clone(),
			tangram_path: arg.tangram_path.clone(),
		});
		server.serve_url(&arg.url).await?;
		Ok::<_, tg::Error>(())
	})?;

	Ok(())
}
