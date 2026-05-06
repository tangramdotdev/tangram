use {std::path::PathBuf, tangram_client::prelude::*, tangram_uri::Uri};

#[derive(Clone, Debug)]
pub struct Arg {
	pub library_paths: Vec<PathBuf>,
	pub listen: bool,
	pub output_path: PathBuf,
	pub tangram_path: PathBuf,
	pub url: Uri,
}

pub async fn run(arg: &Arg) -> tg::Result<()> {
	let server = crate::server::Server::new(crate::server::Arg {
		library_paths: arg.library_paths.clone(),
		output_path: arg.output_path.clone(),
		tangram_path: arg.tangram_path.clone(),
	});
	if arg.listen {
		let listener = crate::server::Server::listen(&arg.url).await?;
		server.serve(listener).await;
	} else {
		let stream = crate::server::Server::connect(&arg.url).await?;
		server.serve_stream(stream).await;
	}
	Ok(())
}
