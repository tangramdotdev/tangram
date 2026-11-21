use {
	super::storage::Set, std::collections::BTreeSet, tangram_client::prelude::*,
	tangram_util::serde::is_default,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	/// The account for which to put a storage entry for the process, after the process is accepted.
	#[tangram_serialize(default, id = 23, skip_serializing_if = "Option::is_none")]
	pub account: Option<crate::usage::Account>,

	/// The boot of the process's sandbox that the process belongs to, which only its holder sets and which never changes.
	#[tangram_serialize(default, id = 24, skip_serializing_if = "Option::is_none")]
	pub boot: Option<u64>,

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
	pub error: Option<Option<Vec<tg::object::Id>>>,

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
	pub log: Option<Option<tg::object::Id>>,

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
	pub output: Option<Option<Vec<tg::object::Id>>>,

	#[tangram_serialize(id = 8)]
	pub parent: Option<tg::process::Id>,

	/// Permissions granted after validation. The process's own put grants them even if its data loses, so that permissions merge as a union, and another principal's put grants them only if the process is new or the put carries both its data and its children.
	#[tangram_serialize(id = 18)]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 19)]
	pub principal: tg::Principal,

	/// Register sandbox membership during verified process initialization; ordinary writes leave this unset.
	#[tangram_serialize(id = 9)]
	pub sandbox: Option<tg::sandbox::Id>,

	/// The position of the write among the writes of the process's holders, so that the index applies their lagging writes in order.
	#[tangram_serialize(default, id = 25, skip_serializing_if = "is_default")]
	pub sequence: u64,

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

	/// The existing process's data, when `reads_data` is true.
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

	/// Whether the merge reads the existing data, which it compares with the arg's data and finishes if a takeover ended the process's boot, given the record of the sandbox that `context_sandbox` names.
	#[must_use]
	pub fn reads_data(
		&self,
		existing: Option<&super::Process>,
		sandbox: Option<&crate::sandbox::Sandbox>,
	) -> bool {
		let status = existing.and_then(|existing| existing.status);
		let compares = self.data.is_some()
			&& status.is_some_and(|status| {
				self.principal != tg::Principal::Process(self.id.clone()) || status.is_finished()
			});
		let finishes = status.is_some_and(|status| !status.is_finished())
			&& !self.supersedes(existing)
			&& self
				.boot
				.or_else(|| existing.and_then(|existing| existing.boot))
				.is_some_and(|boot| boot_taken_over(sandbox, boot));
		compares || finishes
	}

	/// Whether the merge compares the arg's children with the existing children, which the index reads for it.
	#[must_use]
	pub fn compares_children(&self, existing: Option<&super::Process>) -> bool {
		existing.is_some_and(|existing| existing.set.children)
			&& !self.principal.is_root()
			&& self.principal != tg::Principal::Process(self.id.clone())
			&& self.children.is_some()
	}

	/// The sandbox whose record the merge reads, which is the sandbox of a process with a known boot that the put leaves unfinished, whose boot a takeover may have ended. The read is skipped when the put finishes the process or the index already has it finished.
	#[must_use]
	pub fn context_sandbox(&self, existing: Option<&super::Process>) -> Option<tg::sandbox::Id> {
		let finished = self
			.data
			.as_ref()
			.is_some_and(|data| data.status.is_finished())
			|| existing
				.and_then(|existing| existing.status)
				.is_some_and(|status| status.is_finished());
		let boot = self
			.boot
			.or_else(|| existing.and_then(|existing| existing.boot));
		if finished || boot.is_none() {
			return None;
		}
		self.data
			.as_ref()
			.and_then(|data| data.sandbox.clone())
			.or_else(|| self.sandbox.clone())
			.or_else(|| existing.and_then(|existing| existing.sandbox.clone()))
	}

	/// Whether the arg's data supersedes the existing record's data. The first end to arrive is final, an end supersedes unfinished data, and otherwise a write supersedes the writes with lower sequences, and at an equal sequence a later status.
	#[must_use]
	fn supersedes(&self, existing: Option<&super::Process>) -> bool {
		let Some(data) = &self.data else {
			return false;
		};
		let Some(status) = existing.and_then(|existing| existing.status) else {
			return true;
		};
		if status.is_finished() {
			return false;
		}
		if data.status.is_finished() {
			return true;
		}
		let sequence = existing.map_or(0, |existing| existing.sequence);
		(self.sequence, rank(data.status)) > (sequence, rank(status))
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

		// Reject a write that changes the boot of a process whose record has one.
		let existing_boot = existing.and_then(|existing| existing.boot);
		if let (Some(existing_boot), Some(boot)) = (existing_boot, self.boot)
			&& existing_boot != boot
		{
			return Ok(Err(tg::error!(
				id = %self.id,
				"cannot change the boot of an existing process"
			)));
		}
		let boot = existing_boot.or(self.boot);

		// Put the permissions of every accepted put of the process itself, even one whose data loses, so that permissions merge as a union. Another principal's put proves what it grants only if the process is new or the put carries both its data and its children.
		let permissions = self.principal == tg::Principal::Process(self.id.clone())
			|| existing.is_none()
			|| (self.data.is_some() && self.children.is_some());

		// Keep the stored data unless the arg's data supersedes it.
		let superseded = self.supersedes(existing);
		let mut arg = std::borrow::Cow::Borrowed(self);
		if !superseded {
			arg.to_mut().data = None;
		}
		let arg = arg.as_ref();
		let sequence = if superseded {
			arg.sequence
		} else {
			existing.map_or(arg.sequence, |existing| existing.sequence)
		};
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

		// Record a process whose boot a takeover ended as finished, because no holder will finish it. A boot that its live holder ended is left alone, because the holder sends the process's end itself.
		let unfinished = data.as_ref().map_or_else(
			|| {
				existing
					.and_then(|existing| existing.status)
					.is_some_and(|status| !status.is_finished())
			},
			|data| !data.status.is_finished(),
		);
		if unfinished
			&& let Some(boot) = boot
			&& boot_taken_over(context.sandbox, boot)
			&& let Some(mut finished) = data.clone().or_else(|| context.data.cloned())
		{
			let error = context
				.sandbox
				.and_then(|sandbox| sandbox.data.as_ref())
				.and_then(|data| data.data.error.clone())
				.unwrap_or_else(|| {
					tg::Either::Left(tg::error::Data {
						code: Some(tg::error::Code::Cancellation),
						message: Some("the process's sandbox ended".to_owned()),
						..Default::default()
					})
				})
				.map_right(tg::Referent::with_node);
			finished.cacheable = false;
			finished.children = None;
			finished.exit = Some(1);
			finished.finished_at = Some(arg.touched_at);
			finished.set_error(Some(error));
			finished.status = tg::process::Status::Finished;
			data = Some(finished);
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
			|| data.is_some()
			|| existing.is_none_or(|existing| {
				existing.boot != boot
					|| existing.location != location
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
			boot,
			command_id: arg.command_id.clone(),
			creator,
			location,
			metadata,
			owner,
			reference_count: existing.map_or(0, |existing| existing.reference_count),
			sandbox,
			sequence,
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

		// Accept the process's own writes in any order, which the merge orders, keeping the first end that arrives.
		if self.principal == tg::Principal::Process(self.id.clone()) {
			return Ok(Ok(()));
		}
		if self.command_id != existing.command_id {
			return Ok(Err(
				tg::error!(id = %self.id, "cannot replace an existing process"),
			));
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

/// Whether a sandbox's record shows that a takeover ended the boot.
fn boot_taken_over(sandbox: Option<&crate::sandbox::Sandbox>, boot: u64) -> bool {
	sandbox.is_some_and(|sandbox| sandbox.taken_over.contains(&boot))
}

/// The rank of a status, which orders writes at an equal sequence.
fn rank(status: tg::process::Status) -> u8 {
	match status {
		tg::process::Status::Finished => 2,
		tg::process::Status::Queued => 0,
		tg::process::Status::Started => 1,
	}
}
