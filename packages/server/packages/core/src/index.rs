use {
	crate::{Server, Session},
	futures::Stream,
	tangram_client::prelude::*,
	tangram_index as index,
};

pub use tangram_server_index::*;

impl Server {
	pub async fn index_batch(&self, arg: index::batch::Arg) -> tg::Result<()> {
		self.components.indexer.index_batch(self, arg).await
	}

	pub async fn index_batch_inner(&self, arg: index::batch::Arg) -> tg::Result<tg::Result<()>> {
		self.components.indexer.index_batch_inner(self, arg).await
	}
}

impl Session {
	pub async fn index(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + use<>> {
		self.server.components.indexer.index(self).await
	}
}
