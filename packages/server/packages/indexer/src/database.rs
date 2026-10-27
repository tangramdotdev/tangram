use {
	crate::Component,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_server_core::{Server, database::Transaction},
};

mod queue;

impl Component {
	pub(crate) async fn enqueue_database_index_queue_with_transaction(
		server: &Server,
		transaction: &Transaction<'_>,
		arg: &tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		tangram_server_core::database::index::queue::enqueue_database_index_queue_with_transaction(
			&server.config,
			transaction,
			arg,
		)
		.await
	}

	pub(crate) fn spawn_publish_database_index_queue_notification_task(server: &Server) {
		let regions = tangram_server_core::database::index::queue::database_index_queue_regions(
			&server.config,
		);
		for region in regions {
			let region = (!region.is_empty()).then_some(region);
			let subject = database_index_queue_subject();
			server.notifications.notify_region(region, subject);
		}
	}
}

#[must_use]
fn database_index_queue_subject() -> String {
	"database.index.queue".to_owned()
}
