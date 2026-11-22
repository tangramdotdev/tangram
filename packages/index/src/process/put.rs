use {super::storage::Set, std::collections::BTreeSet, tangram_client::prelude::*};

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
