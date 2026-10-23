use {futures::FutureExt as _, std::sync::Arc, tangram_client::prelude::*};

pub use {
	tangram_server_checkin as checkin,
	tangram_server_core::{
		self as core, Components, Config, Owned, Server, Shared, Shutdown, config,
	},
};

/// Create the components.
pub fn components(arg: &tangram_server_core::component::Arg<'_>) -> tg::Result<Components> {
	let authentication = Arc::new(tangram_server_authentication::Component::new(arg));
	let authorization = Arc::new(tangram_server_authorization::Component::new());
	let billing = Arc::new(tangram_server_billing::Component::new(arg));
	let check = Arc::new(tangram_server_check::Component::new());
	let checkin = Arc::new(tangram_server_checkin::Component::new());
	let checkout = Arc::new(tangram_server_checkout::Component::new());
	let checkpoint = Arc::new(tangram_server_checkpoint::Component::new());
	let checksum = Arc::new(tangram_server_checksum::Component::new());
	let children = Arc::new(tangram_server_children::Component::new());
	let clean = Arc::new(tangram_server_clean::Component::new());
	let clock = Arc::new(tangram_server_clock::Component::new());
	let document = Arc::new(tangram_server_document::Component::new());
	let format = Arc::new(tangram_server_format::Component::new());
	let get = Arc::new(tangram_server_get::Component::new());
	let grant = Arc::new(tangram_server_grant::Component::new());
	let group = Arc::new(tangram_server_group::Component::new());
	let health = Arc::new(tangram_server_health::Component::new());
	let indexer = Arc::new(tangram_server_indexer::Component::new(arg));
	let list = Arc::new(tangram_server_list::Component::new());
	let log = Arc::new(tangram_server_log::Component::new());
	let lsp = Arc::new(tangram_server_lsp::Component::new());
	let match_ = Arc::new(tangram_server_match::Component::new());
	let module = Arc::new(tangram_server_module::Component::new());
	let node = Arc::new(tangram_server_node::Component::new());
	let object = Arc::new(tangram_server_object::Component::new());
	let organization = Arc::new(tangram_server_organization::Component::new());
	let process = Arc::new(tangram_server_process::Component::new(arg)?);
	let pull = Arc::new(tangram_server_pull::Component::new());
	let push = Arc::new(tangram_server_push::Component::new());
	let read = Arc::new(tangram_server_read::Component::new());
	let remote = Arc::new(tangram_server_remote::Component::new());
	let runner = Arc::new(tangram_server_runner::Component::new(arg)?);
	let sandbox = Arc::new(tangram_server_sandbox::Component::new(arg)?);
	let scheduler = Arc::new(tangram_server_scheduler::Component::new(arg));
	let sync = Arc::new(tangram_server_sync::Component::new(arg)?);
	let tag = Arc::new(tangram_server_tag::Component::new());
	let user = Arc::new(tangram_server_user::Component::new());
	let verify = Arc::new(tangram_server_verify::Component::new());
	let vfs = Arc::new(tangram_server_vfs::Component::new());
	let watch = Arc::new(tangram_server_watch::Component::new());
	let write = Arc::new(tangram_server_write::Component::new());
	let components = Components {
		authentication,
		authorization,
		billing,
		check,
		checkin,
		checkout,
		checkpoint,
		checksum,
		children,
		clean,
		clock,
		document,
		format,
		get,
		grant,
		group,
		health,
		indexer,
		list,
		log,
		lsp,
		match_,
		module,
		node,
		object,
		organization,
		process,
		pull,
		push,
		read,
		remote,
		runner,
		sandbox,
		scheduler,
		sync,
		tag,
		user,
		verify,
		vfs,
		watch,
		write,
	};

	Ok(components)
}

/// Create the components and start a server.
pub async fn start(config: Config) -> tg::Result<Owned> {
	let owned = Server::start(config, components).boxed().await?;

	Ok(owned)
}
