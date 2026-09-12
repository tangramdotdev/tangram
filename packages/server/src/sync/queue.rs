use tangram_client::prelude::*;

pub struct Queue {
	database: async_channel::Sender<DatabaseNode>,
	object: async_channel::Sender<ObjectNode>,
	process: async_channel::Sender<ProcessNode>,
	sandbox: async_channel::Sender<SandboxNode>,
}

pub struct DatabaseNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::Id,
	pub local_tokens: tg::tokens::Entry,
	pub remote_tokens: tg::tokens::Entry,
	pub selector: tg::Selector<tg::Id>,
}

pub struct ObjectNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<ObjectKind>,
	pub local_tokens: tg::tokens::Entry,
	pub parent: Option<tg::Id>,
	pub remote_tokens: tg::tokens::Entry,
}

pub struct ProcessNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::process::Id,
	pub local_tokens: tg::tokens::Entry,
	pub parent: Option<tg::process::Id>,
	pub remote_tokens: tg::tokens::Entry,
}

pub struct SandboxNode {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::sandbox::Id,
	pub local_tokens: tg::tokens::Entry,
	pub remote_tokens: tg::tokens::Entry,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObjectKind {
	Command,
	Error,
	Log,
	Output,
}

impl Queue {
	pub fn new(
		database_sender: async_channel::Sender<DatabaseNode>,
		object_sender: async_channel::Sender<ObjectNode>,
		process_sender: async_channel::Sender<ProcessNode>,
		sandbox_sender: async_channel::Sender<SandboxNode>,
	) -> Self {
		Self {
			database: database_sender,
			object: object_sender,
			process: process_sender,
			sandbox: sandbox_sender,
		}
	}

	pub fn enqueue(
		&self,
		eager: bool,
		id: tg::Id,
		local_tokens: tg::tokens::Entry,
		remote_tokens: tg::tokens::Entry,
	) -> tg::Result<()> {
		self.enqueue_with_descendants(true, eager, id, local_tokens, remote_tokens)
	}

	pub fn enqueue_with_descendants(
		&self,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		local_tokens: tg::tokens::Entry,
		remote_tokens: tg::tokens::Entry,
	) -> tg::Result<()> {
		match id.kind() {
			tg::id::Kind::Group
			| tg::id::Kind::Organization
			| tg::id::Kind::Tag
			| tg::id::Kind::User => {
				let selector = tg::Selector::Id(id.clone());
				self.enqueue_database(DatabaseNode {
					descendants,
					eager,
					id,
					local_tokens,
					remote_tokens,
					selector,
				});
			},
			tg::id::Kind::Process => {
				self.enqueue_process(ProcessNode {
					descendants,
					eager,
					id: id.try_into()?,
					local_tokens,
					parent: None,
					remote_tokens,
				});
			},
			tg::id::Kind::Sandbox => {
				self.enqueue_sandbox(SandboxNode {
					descendants,
					eager,
					id: id.try_into()?,
					local_tokens,
					remote_tokens,
				});
			},
			_ => {
				let id = tg::object::Id::try_from(id)
					.map_err(|_| tg::error!("invalid sync node kind"))?;
				self.enqueue_object(ObjectNode {
					descendants,
					eager,
					id,
					kind: None,
					local_tokens,
					parent: None,
					remote_tokens,
				});
			},
		}

		Ok(())
	}

	pub fn enqueue_database(&self, node: DatabaseNode) {
		self.database.force_send(node).ok();
	}

	pub fn enqueue_object(&self, node: ObjectNode) {
		self.object.force_send(node).ok();
	}

	pub fn enqueue_process(&self, node: ProcessNode) {
		self.process.force_send(node).ok();
	}

	pub fn enqueue_objects(&self, nodes: impl IntoIterator<Item = ObjectNode>) {
		let nodes: Vec<_> = nodes.into_iter().collect();
		for node in nodes {
			self.object.force_send(node).ok();
		}
	}

	pub fn enqueue_sandbox(&self, node: SandboxNode) {
		self.sandbox.force_send(node).ok();
	}

	pub fn close(&self) {
		self.database.close();
		self.object.close();
		self.process.close();
		self.sandbox.close();
	}
}
