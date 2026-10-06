use {super::storage::Set, std::collections::BTreeSet, tangram_client::prelude::*};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 13)]
	pub cached: bool,

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

	/// Permissions granted after validation if the process is new or both `data` and `children` are `Some`.
	#[tangram_serialize(id = 18)]
	pub permissions: Vec<crate::permission::put::Arg>,

	#[tangram_serialize(id = 19)]
	pub principal: tg::Principal,

	/// Register sandbox membership during verified process initialization; ordinary writes leave this unset.
	#[tangram_serialize(id = 9)]
	pub sandbox: Option<tg::sandbox::Id>,

	#[tangram_serialize(id = 10)]
	pub storage: Set,

	#[tangram_serialize(id = 11)]
	pub time_to_touch: std::time::Duration,

	#[tangram_serialize(id = 12)]
	pub touched_at: i64,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		if !self.principal.is_root()
			&& self.principal != tg::Principal::Process(self.id.clone())
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

	pub fn validate_existing(&self, existing: &super::Process) -> tg::Result<tg::Result<()>> {
		if self.principal.is_root() || self.principal == tg::Principal::Process(self.id.clone()) {
			return Ok(Ok(()));
		}
		if self.command_id != existing.command_id {
			return Ok(Err(tg::error!("cannot replace an existing process")));
		}
		if let Some(data) = &self.data {
			let Some(existing) = &existing.data else {
				return Ok(Err(tg::error!("cannot verify the existing process data")));
			};
			let mut data = data.clone().without_location_and_tokens();
			data.children = None;
			let mut existing = existing.clone().without_location_and_tokens();
			existing.children = None;
			let data = serde_json::to_value(data)
				.map_err(|error| tg::error!(!error, "failed to serialize the process contents"))?;
			let existing = serde_json::to_value(existing)
				.map_err(|error| tg::error!(!error, "failed to serialize the process contents"))?;
			if data != existing {
				return Ok(Err(tg::error!("cannot replace an existing process")));
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
