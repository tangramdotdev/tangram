use {
	futures::FutureExt as _,
	tangram_client::prelude::*,
	tangram_server_core::{Components, Server},
};

pub use tangram_server_core::{Config, Owned, Shutdown, config};

/// Create the components and start a server.
pub async fn start(config: Config) -> tg::Result<Owned> {
	let owned = Server::start(config, components).boxed().await?;

	Ok(owned)
}

/// Create the components.
fn components(arg: &tangram_server_core::component::Arg<'_>) -> tg::Result<Components> {
	let authentication = Box::new(tangram_server_authentication::Component::new(arg));
	let authorization = Box::new(tangram_server_authorization::Component::new());
	let billing = Box::new(tangram_server_billing::Component::new(arg));
	let check = Box::new(tangram_server_check::Component::new());
	let checkin = Box::new(tangram_server_checkin::Component::new());
	let checkout = Box::new(tangram_server_checkout::Component::new());
	let checkpoint = Box::new(tangram_server_checkpoint::Component::new());
	let checksum = Box::new(tangram_server_checksum::Component::new());
	let children = Box::new(tangram_server_children::Component::new());
	let clean = Box::new(tangram_server_clean::Component::new());
	let clock = Box::new(tangram_server_clock::Component::new());
	let document = Box::new(tangram_server_document::Component::new());
	let format = Box::new(tangram_server_format::Component::new());
	let get = Box::new(tangram_server_get::Component::new());
	let grant = Box::new(tangram_server_grant::Component::new());
	let group = Box::new(tangram_server_group::Component::new());
	let health = Box::new(tangram_server_health::Component::new());
	let indexer = Box::new(tangram_server_indexer::Component::new(arg));
	let list = Box::new(tangram_server_list::Component::new());
	let log = Box::new(tangram_server_log::Component::new());
	let lsp = Box::new(tangram_server_lsp::Component::new());
	let match_ = Box::new(tangram_server_match::Component::new());
	let module = Box::new(tangram_server_module::Component::new());
	let node = Box::new(tangram_server_node::Component::new());
	let object = Box::new(tangram_server_object::Component::new());
	let organization = Box::new(tangram_server_organization::Component::new());
	let process = Box::new(tangram_server_process::Component::new(arg)?);
	let pull = Box::new(tangram_server_pull::Component::new());
	let push = Box::new(tangram_server_push::Component::new());
	let read = Box::new(tangram_server_read::Component::new());
	let remote = Box::new(tangram_server_remote::Component::new());
	let runner = Box::new(tangram_server_runner::Component::new(arg)?);
	let sandbox = Box::new(tangram_server_sandbox::Component::new(arg)?);
	let scheduler = Box::new(tangram_server_scheduler::Component::new(arg));
	let sync = Box::new(tangram_server_sync::Component::new(arg)?);
	let tag = Box::new(tangram_server_tag::Component::new());
	let usage = Box::new(tangram_server_usage::Component::new());
	let user = Box::new(tangram_server_user::Component::new());
	let verify = Box::new(tangram_server_verify::Component::new());
	let vfs = Box::new(tangram_server_vfs::Component::new());
	let watch = Box::new(tangram_server_watch::Component::new());
	let write = Box::new(tangram_server_write::Component::new());
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
		usage,
		user,
		verify,
		vfs,
		watch,
		write,
	};

	Ok(components)
}
