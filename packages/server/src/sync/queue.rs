use tangram_client::prelude::*;

pub struct Queue {
	database: async_channel::Sender<DatabaseItem>,
	object: async_channel::Sender<ObjectItem>,
	process: async_channel::Sender<ProcessItem>,
	sandbox: async_channel::Sender<SandboxItem>,
}

pub struct DatabaseItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::Id,
	pub selector: tg::Selector<tg::Id>,
	pub token: Option<tg::grant::Token>,
}

pub struct ObjectItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::object::Id,
	pub kind: Option<ObjectKind>,
	pub parent: Option<tg::Id>,
	pub token: Option<tg::grant::Token>,
}

pub struct ProcessItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::process::Id,
	pub parent: Option<tg::process::Id>,
	pub token: Option<tg::grant::Token>,
}

pub struct SandboxItem {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::sandbox::Id,
	pub token: Option<tg::grant::Token>,
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
		database_sender: async_channel::Sender<DatabaseItem>,
		object_sender: async_channel::Sender<ObjectItem>,
		process_sender: async_channel::Sender<ProcessItem>,
		sandbox_sender: async_channel::Sender<SandboxItem>,
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
		token: Option<tg::grant::Token>,
	) -> tg::Result<()> {
		self.enqueue_with_descendants(true, eager, id, token)
	}

	pub fn enqueue_with_descendants(
		&self,
		descendants: bool,
		eager: bool,
		id: tg::Id,
		token: Option<tg::grant::Token>,
	) -> tg::Result<()> {
		match id.kind() {
			tg::id::Kind::Group
			| tg::id::Kind::Organization
			| tg::id::Kind::Tag
			| tg::id::Kind::User => {
				let selector = tg::Selector::Id(id.clone());
				self.enqueue_database(DatabaseItem {
					descendants,
					eager,
					id,
					selector,
					token,
				});
			},
			tg::id::Kind::Process => {
				self.enqueue_process(ProcessItem {
					descendants,
					eager,
					id: id.try_into()?,
					parent: None,
					token,
				});
			},
			tg::id::Kind::Sandbox => {
				self.enqueue_sandbox(SandboxItem {
					descendants,
					eager,
					id: id.try_into()?,
					token,
				});
			},
			_ => {
				let id = tg::object::Id::try_from(id)
					.map_err(|_| tg::error!("invalid sync item kind"))?;
				self.enqueue_object(ObjectItem {
					descendants,
					eager,
					id,
					kind: None,
					parent: None,
					token,
				});
			},
		}

		Ok(())
	}

	pub fn enqueue_database(&self, item: DatabaseItem) {
		self.database.force_send(item).ok();
	}

	pub fn enqueue_object(&self, item: ObjectItem) {
		self.object.force_send(item).ok();
	}

	pub fn enqueue_process(&self, item: ProcessItem) {
		self.process.force_send(item).ok();
	}

	pub fn enqueue_objects(&self, items: impl IntoIterator<Item = ObjectItem>) {
		let items: Vec<_> = items.into_iter().collect();
		for item in items {
			self.object.force_send(item).ok();
		}
	}

	pub fn enqueue_processes(&self, items: impl IntoIterator<Item = ProcessItem>) {
		let items: Vec<_> = items.into_iter().collect();
		for item in items {
			self.process.force_send(item).ok();
		}
	}

	pub fn enqueue_sandbox(&self, item: SandboxItem) {
		self.sandbox.force_send(item).ok();
	}

	pub fn close(&self) {
		self.database.close();
		self.object.close();
		self.process.close();
		self.sandbox.close();
	}
}
