use {super::storage::Set, std::collections::BTreeSet, tangram_client::prelude::*};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	/// The account for which to put a storage entry for the process, after the process is accepted.
	#[tangram_serialize(default, id = 23, skip_serializing_if = "Option::is_none")]
	pub account: Option<crate::usage::Account>,

	#[tangram_serialize(id = 13)]
	pub cached: bool,

	/// The captures to put after the process, only if the process put is accepted.
	#[tangram_serialize(
		default,
		id = 21,
		skip_serializing_if = "crate::permission::capture::Captures::is_empty"
	)]
	pub captures: crate::permission::capture::Captures,

	#[tangram_serialize(id = 0)]
	pub children: Option<Vec<tg::process::data::Child>>,

	#[tangram_serialize(default, id = 17, skip_serializing_if = "Option::is_none")]
	pub command: Option<Vec<tg::object::Id>>,

	#[tangram_serialize(id = 1)]
	pub command_id: tg::object::Id,

	#[tangram_serialize(id = 2)]
	pub data: Option<tg::process::Data>,

	#[tangram_serialize(
		default,
		id = 3,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub error: Option<tg::update::Update<Vec<tg::object::Id>>>,

	#[tangram_serialize(id = 4)]
	pub id: tg::process::Id,

	/// The process location, or `None` to preserve the indexed location.
	#[tangram_serialize(id = 15)]
	pub location: Option<tg::Location>,

	#[tangram_serialize(
		default,
		id = 5,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub log: Option<tg::update::Update<tg::object::Id>>,

	#[tangram_serialize(id = 6)]
	pub metadata: tg::process::Metadata,

	#[tangram_serialize(id = 14)]
	pub options: tg::referent::Options,

	#[tangram_serialize(
		default,
		id = 7,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub output: Option<tg::update::Update<Vec<tg::object::Id>>>,

	#[tangram_serialize(id = 8)]
	pub parent: Option<tg::process::Id>,

	/// Permissions granted after validation if the process is new, if both `data` and `children` are `Some`, or if the put is a queued record that arrives after the process started.
	#[tangram_serialize(id = 18)]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 19)]
	pub principal: tg::Principal,

	/// The registration that must still own the runner when this mutation is applied.
	#[tangram_serialize(default, id = 20, skip_serializing_if = "Option::is_none")]
	pub registration: Option<crate::runner::Registration>,

	/// Register sandbox membership during verified process initialization; ordinary writes leave this unset.
	#[tangram_serialize(id = 9)]
	pub sandbox: Option<tg::sandbox::Id>,

	/// The boot that the process's sandbox must still be at when this put is applied, which makes the put a conditional finish of a queued process of a boot that ended, so that a process queued for a later boot is not finished.
	#[tangram_serialize(default, id = 22, skip_serializing_if = "Option::is_none")]
	pub sandbox_boot: Option<SandboxBoot>,

	#[tangram_serialize(id = 10)]
	pub storage: Set,

	#[tangram_serialize(id = 11)]
	pub time_to_touch: std::time::Duration,

	#[tangram_serialize(id = 12)]
	pub touched_at: i64,
}

/// The records that a process put reads from the index for its merge, besides the existing process.
#[derive(Clone, Copy, Debug, Default)]
pub struct Context<'a> {
	/// Whether the indexed parent accepts the process as a child, or `None` if the put has no parent or the parent is not indexed.
	pub accepts_child: Option<bool>,

	/// The first children of the existing process, when `compares_children` is true.
	pub children: Option<&'a [tg::process::data::Child]>,

	/// The existing process's data, when `compares_data` is true.
	pub data: Option<&'a tg::process::Data>,

	/// The record of the sandbox that `context_sandbox` names.
	pub sandbox: Option<&'a crate::sandbox::Sandbox>,
}

/// What a process put writes to the index.
pub struct Merge {
	/// Whether the index puts the arg's permissions.
	pub permissions: bool,

	/// The record to write, or `None` if the put leaves the record as it is.
	pub update: Option<Update>,
}

/// The record that a process put writes, and the relationships that it adds.
pub struct Update {
	/// The lists that the put sets for the first time, whose children or objects the index writes.
	pub added: super::Set,

	/// Whether the record, its relationships, or its data changed, which enqueues an update of the process.
	pub changed: bool,

	/// The data to store, which replaces the stored data.
	pub data: Option<tg::process::Data>,

	/// Whether the index links the process to its parent.
	pub parent: bool,

	pub process: super::Process,
}

/// A sandbox's boot that a conditional process put requires.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct SandboxBoot {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub sandbox: tg::sandbox::Id,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		if self.principal != tg::Principal::Process(self.id.clone())
			&& self
				.data
				.as_ref()
				.is_some_and(|data| !data.status.is_finished())
		{
			return Err(tg::error!("expected a finished process"));
		}
		let Some(children) = &self.children else {
			return Ok(());
		};
		let mut ids = BTreeSet::new();
		for child in children {
			if !ids.insert(&child.process.node) {
				return Err(tg::error!("the process children must be unique"));
			}
		}

		Ok(())
	}

	/// Reject a conditional put unless the sandbox is at the boot that the put names, in any state, because a later boot's queued processes are not the ended boot's.
	pub fn validate_sandbox_boot(
		&self,
		sandbox: Option<&crate::sandbox::Sandbox>,
	) -> tg::Result<()> {
		let Some(expected) = &self.sandbox_boot else {
			return Ok(());
		};
		let boot = sandbox
			.and_then(|sandbox| sandbox.data.as_ref())
			.map(|data| data.data.boot);
		if boot != Some(expected.boot) {
			let boot = expected.boot;
			return Err(tg::error!(
				id = %self.id,
				sandbox = %expected.sandbox,
				%boot,
				"the process's sandbox is not at the boot"
			));
		}

		Ok(())
	}

	/// Whether the merge compares the arg's data with the existing data, which the index reads for it.
	#[must_use]
	pub fn compares_data(&self, existing: Option<&super::Process>) -> bool {
		self.data.is_some()
			&& existing
				.and_then(|existing| existing.status)
				.is_some_and(|status| {
					self.principal != tg::Principal::Process(self.id.clone())
						|| status.is_finished()
				})
	}

	/// Whether the merge compares the arg's children with the existing children, which the index reads for it.
	#[must_use]
	pub fn compares_children(&self, existing: Option<&super::Process>) -> bool {
		existing.is_some_and(|existing| existing.set.children)
			&& !self.principal.is_root()
			&& self.principal != tg::Principal::Process(self.id.clone())
			&& self.children.is_some()
	}

	/// The sandbox whose record the merge reads, if any.
	#[must_use]
	pub fn context_sandbox(&self) -> Option<&tg::sandbox::Id> {
		self.sandbox_boot.as_ref().map(|expected| &expected.sandbox)
	}

	/// Merge the put with the existing record, which is the one rule that every index backend applies, so that writes applied in any order produce the same record.
	pub fn merge(
		&self,
		existing: Option<&super::Process>,
		context: Context<'_>,
	) -> tg::Result<tg::Result<Merge>> {
		// Validate the arg.
		if let Err(error) = self.validate() {
			return Ok(Err(error));
		}

		// Reject a conditional finish unless the process's sandbox is still at the boot that the put names.
		if let Err(error) = self.validate_sandbox_boot(context.sandbox) {
			return Ok(Err(error));
		}

		// Ignore a queued record that arrives after the process started, except for its permissions, because writes may be applied out of order.
		if self
			.data
			.as_ref()
			.is_some_and(|data| data.status.is_queued())
			&& existing.is_some_and(|existing| existing.status.is_some())
		{
			let merge = Merge {
				permissions: true,
				update: None,
			};
			return Ok(Ok(merge));
		}

		// Validate the process data and children against the existing record.
		if let Some(existing) = existing {
			if let Err(error) = self.validate_existing(existing, context.data)? {
				return Ok(Err(error));
			}
			if !self.principal.is_root()
				&& self.principal != tg::Principal::Process(self.id.clone())
				&& self.children.is_some()
			{
				if !existing.set.children {
					return Ok(Err(tg::error!(
						"cannot verify the existing process children"
					)));
				}
				if let Err(error) = self.validate_children(context.children.unwrap_or_default())? {
					return Ok(Err(error));
				}
			}
		}

		// Put the permissions if the process is new or the put carries both its data and its children.
		let permissions = existing.is_none() || (self.data.is_some() && self.children.is_some());

		// Preserve the stored data after validating an identical write.
		let mut arg = std::borrow::Cow::Borrowed(self);
		if let Some(status) = existing.and_then(|existing| existing.status)
			&& (arg.principal != tg::Principal::Process(arg.id.clone()) || status.is_finished())
		{
			arg.to_mut().data = None;
		}
		let arg = arg.as_ref();
		let merge = !arg.complete();

		// Touch the process unless it was touched within the time to touch.
		let time_to_touch = i64::try_from(arg.time_to_touch.as_secs()).unwrap();
		let touch = existing.is_none_or(|existing| {
			arg.touched_at.saturating_sub(existing.touched_at) >= time_to_touch
		});
		let touched_at = existing.map_or(arg.touched_at, |existing| {
			if touch {
				existing.touched_at.max(arg.touched_at)
			} else {
				existing.touched_at
			}
		});

		// Get the lists that the put sets for the first time.
		let existing_set = existing.map(|existing| &existing.set);
		let added = super::Set {
			children: arg.children.is_some() && existing_set.is_none_or(|set| !set.children),
			command_objects: arg.command.is_some()
				&& existing_set.is_none_or(|set| !set.command_objects),
			error_objects: arg.error.is_some() && existing_set.is_none_or(|set| !set.error_objects),
			log_objects: arg.log.is_some() && existing_set.is_none_or(|set| !set.log_objects),
			output_objects: arg.output.is_some()
				&& existing_set.is_none_or(|set| !set.output_objects),
		};

		// Link the process to its parent if the indexed parent accepts it, which it does unless the parent finished without listing it.
		let parent = arg.parent.is_some() && context.accepts_child == Some(true);

		// Merge the set, storage, and metadata with the existing record unless the put is complete.
		let mut set = arg.set();
		if merge && let Some(existing) = existing {
			set.merge(&existing.set);
		}
		let mut storage = arg.storage;
		if merge && let Some(existing) = existing {
			storage.insert(existing.storage);
		}
		let mut metadata = arg.metadata.clone();
		if merge && let Some(existing) = existing {
			metadata.merge(&existing.metadata);
		}

		// Take the status, creator, owner, and sandbox from the data, or keep the existing ones.
		let mut data = arg.data.clone();
		if let Some(data) = &mut data {
			data.children = None;
		}
		let status = data
			.as_ref()
			.map(|data| data.status)
			.or_else(|| existing.and_then(|existing| existing.status));
		let creator = data.as_ref().map_or_else(
			|| existing.map_or_default(|existing| existing.creator.clone()),
			|data| data.creator.clone(),
		);
		let owner = data.as_ref().map_or_else(
			|| existing.map_or_default(|existing| existing.owner.clone()),
			|data| data.owner.clone(),
		);
		let location = arg
			.location
			.clone()
			.or_else(|| existing.and_then(|existing| existing.location.clone()));
		let sandbox = arg.data.as_ref().map_or_else(
			|| {
				arg.sandbox
					.clone()
					.or_else(|| existing.and_then(|existing| existing.sandbox.clone()))
			},
			|data| data.sandbox.clone(),
		);

		// Leave the record as it is if nothing changed and the process is not touched.
		let changed = parent
			|| arg.sandbox.is_some()
			|| arg.data.is_some()
			|| existing.is_none_or(|existing| {
				existing.location != location
					|| existing.metadata != metadata
					|| existing.sandbox != sandbox
					|| existing.set != set
					|| existing.storage != storage
			});
		if !changed && !touch {
			let merge = Merge {
				permissions,
				update: None,
			};
			return Ok(Ok(merge));
		}

		// Create the record.
		let process = super::Process {
			command_id: arg.command_id.clone(),
			creator,
			location,
			metadata,
			owner,
			reference_count: existing.map_or(0, |existing| existing.reference_count),
			sandbox,
			set,
			status,
			storage,
			touched_at,
		};
		let update = Update {
			added,
			changed,
			data,
			parent,
			process,
		};
		let merge = Merge {
			permissions,
			update: Some(update),
		};

		Ok(Ok(merge))
	}

	pub fn validate_existing(
		&self,
		existing: &super::Process,
		existing_data: Option<&tg::process::Data>,
	) -> tg::Result<tg::Result<()>> {
		// Reject a write that changes the creator or the owner of a process whose record has them.
		if existing.status.is_some()
			&& let Some(data) = &self.data
			&& (data.creator != existing.creator || data.owner != existing.owner)
		{
			return Ok(Err(tg::error!(
				id = %self.id,
				"cannot change the creator or the owner of an existing process"
			)));
		}

		// Reject a write that changes the sandbox of a process whose record has one.
		if existing.sandbox.is_some()
			&& let Some(data) = &self.data
			&& data.sandbox != existing.sandbox
		{
			return Ok(Err(tg::error!(
				id = %self.id,
				"cannot change the sandbox of an existing process"
			)));
		}

		if self.principal == tg::Principal::Process(self.id.clone())
			&& existing.status.is_none_or(|status| !status.is_finished())
		{
			return Ok(Ok(()));
		}
		if self.command_id != existing.command_id {
			return Ok(Err(
				tg::error!(id = %self.id, "cannot replace an existing process"),
			));
		}

		// Accept the process's own write of its unfinished data after its finished data, which the index can receive out of order, because the put preserves the finished data.
		if self.principal == tg::Principal::Process(self.id.clone())
			&& self.data.as_ref().is_some_and(|data| {
				!data.status.is_finished()
					&& data.error.is_none()
					&& data.exit.is_none()
					&& data.finished_at.is_none()
					&& data.log.is_none()
					&& data.output.is_none()
			}) {
			return Ok(Ok(()));
		}

		if let (Some(data), Some(existing)) = (&self.data, existing_data) {
			let mut data = data.clone().without_location_and_tokens();
			data.children = None;
			let mut existing = existing.clone().without_location_and_tokens();
			existing.children = None;
			let data = serde_json::to_value(data)
				.map_err(|error| tg::error!(!error, "failed to serialize the process contents"))?;
			let existing = serde_json::to_value(existing)
				.map_err(|error| tg::error!(!error, "failed to serialize the process contents"))?;
			if data != existing {
				return Ok(Err(
					tg::error!(id = %self.id, "cannot replace an existing process"),
				));
			}
		}

		Ok(Ok(()))
	}

	pub fn validate_children(
		&self,
		existing: &[tg::process::data::Child],
	) -> tg::Result<tg::Result<()>> {
		let Some(children) = &self.children else {
			return Ok(Ok(()));
		};
		let normalize = |children: &[tg::process::data::Child]| {
			let children = children
				.iter()
				.cloned()
				.map(tg::process::data::Child::without_location_and_tokens)
				.collect::<Vec<_>>();
			serde_json::to_value(children)
				.map_err(|error| tg::error!(!error, "failed to serialize the process contents"))
		};
		if normalize(children)? != normalize(existing)? {
			return Ok(Err(tg::error!(
				"cannot replace the existing process children"
			)));
		}
		Ok(Ok(()))
	}

	#[must_use]
	pub fn complete(&self) -> bool {
		self.set().complete()
			&& self.metadata.subtree.count.is_some()
			&& self.metadata.subtree.depth.is_some()
			&& self.metadata.subtree.command_objects.complete()
			&& self.metadata.subtree.error_objects.complete()
			&& self.metadata.subtree.log_objects.complete()
			&& self.metadata.subtree.output_objects.complete()
			&& self.metadata.node.command_objects.complete()
			&& self.metadata.node.error_objects.complete()
			&& self.metadata.node.log_objects.complete()
			&& self.metadata.node.output_objects.complete()
	}

	#[must_use]
	pub fn set(&self) -> super::Set {
		super::Set {
			children: self.children.is_some(),
			command_objects: self.command.is_some(),
			error_objects: self.error.is_some(),
			log_objects: self.log.is_some(),
			output_objects: self.output.is_some(),
		}
	}
}
