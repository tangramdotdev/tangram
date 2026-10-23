use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub use tangram_server_checkin_graph::{
	Contents, Directory, File, Graph, Node, ObjectOptions, Petgraph, Referrer, Solution, Solutions,
	Symlink, Variant, strip_lock, try_read_lock,
};

pub type Stream = BoxStream<'static, tg::Result<tg::progress::Event<tg::checkin::Output>>>;

#[derive(Clone)]
pub struct TaskOutput {
	pub graph: Graph,
}

/// The checkin component.
pub trait Component: crate::component::Lifecycle {
	fn checkin<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::checkin::Arg,
	) -> BoxFuture<'a, tg::Result<Stream>>;

	fn checkin_request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}

/// Create the ignorer for check-ins.
pub fn create_ignorer() -> tg::Result<tangram_ignore::Ignorer> {
	let file_names = vec![".tangramignore".into(), ".gitignore".into()];
	let global = indoc!(
		"
			.DS_Store
			.git
			.tangram
			tangram.lock
		"
	);
	tangram_ignore::Ignorer::new(file_names, Some(global))
		.map_err(|error| tg::error!(!error, "failed to create the matcher"))
}
