use {crate::Server, std::ops::ControlFlow, tangram_client::prelude::*};

pub use tangram_server_database::*;

impl Server {
	pub async fn enqueue_database_index_queue_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: &tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), Error>> {
		tangram_server_database::index::queue::enqueue_database_index_queue_with_transaction(
			&self.config,
			transaction,
			arg,
		)
		.await
	}
}
