use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

/// The log component.
pub trait Component: crate::component::Lifecycle {
	fn process_log_stream<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		arg: &'a mut tg::process::stdio::read::Arg,
		end: Option<tg::process::stdio::End>,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> BoxFuture<
		'a,
		tg::Result<(
			Option<tg::process::stdio::End>,
			BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
		)>,
	>;
}
