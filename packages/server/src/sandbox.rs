use {
	crate::{Server, Session},
	dashmap::DashMap,
	std::{collections::HashMap, sync::Arc},
	tangram_client::prelude::*,
	tangram_messenger::prelude::*,
};

pub mod control;
pub mod create;
pub mod destroy;
pub mod finalize;
pub mod get;
pub mod isolation;
pub mod list;
pub mod process;
pub mod status;

pub type Map = DashMap<tg::sandbox::Id, State, tg::id::BuildHasher>;

pub struct State {
	pub allocation: Option<Arc<tokio::sync::Mutex<Option<crate::runner::Allocation>>>>,
	pub data: tg::sandbox::get::Output,
	pub processes: HashMap<tg::process::Id, crate::process::State>,
	pub sandbox: Option<tangram_sandbox::Sandbox>,
	pub token: Option<String>,
}

pub type Tasks = tangram_futures::task::Map<tg::sandbox::Id, (), (), tg::id::BuildHasher>;

impl Session {
	pub(super) fn create_sandbox_token_string() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}
}

impl Server {
	pub(crate) fn spawn_publish_sandbox_status_task(&self, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.messenger.publish(subject, ()).await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish the sandbox status message");
				}
			}
		});
	}

	pub(crate) fn validate_sandbox_resources(
		isolation: &tangram_sandbox::Isolation,
		cpu: Option<u64>,
		memory: Option<u64>,
		hostname: Option<&str>,
	) -> tg::Result<()> {
		if cpu == Some(0) {
			return Err(tg::error!("sandbox cpu must be greater than zero"));
		}
		if memory == Some(0) {
			return Err(tg::error!("sandbox memory must be greater than zero"));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_))
			&& (cpu.is_some() || memory.is_some())
		{
			return Err(tg::error!(
				"sandbox cpu and memory are not supported with seatbelt isolation"
			));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_)) && hostname.is_some() {
			return Err(tg::error!(
				"setting a hostname is not supported with seatbelt isolation"
			));
		}
		Ok(())
	}
}
