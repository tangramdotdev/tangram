use {
	crate::prelude::*,
	std::{
		collections::{BTreeMap, BTreeSet},
		path::PathBuf,
	},
};

#[cfg(test)]
mod tests;

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct Command {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub args: Vec<tg::command::data::Value>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: BTreeMap<String, tg::command::data::Value>,

	#[tangram_serialize(id = 3)]
	pub executable: tg::Referent<tg::command::data::Executable>,

	#[tangram_serialize(id = 4)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub stdin: Option<tg::Referent<tg::blob::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,
}

enum Update<'a> {
	ForLocation(&'a tg::Location),
	Inherit(&'a tg::referent::Options),
}

impl Command {
	#[must_use]
	pub fn new(arg: tg::process::spawn::CommandArg, host: String) -> Self {
		Self {
			args: arg.args,
			cwd: arg.cwd,
			env: arg.env,
			executable: arg.executable,
			host,
			stdin: arg.stdin,
			user: arg.user,
		}
	}

	#[must_use]
	pub fn with_command_data(data: tg::command::Data, options: &tg::referent::Options) -> Self {
		let mut command = Self {
			args: data.args,
			cwd: data.cwd,
			env: data.env,
			executable: tg::Referent::with_node(data.executable),
			host: data.host,
			stdin: data.stdin.map(tg::Referent::with_node),
			user: data.user,
		};
		command.inherit_location_and_tokens(options);
		command
	}

	pub fn inherit_location_and_tokens(&mut self, options: &tg::referent::Options) {
		self.update_options(&Update::Inherit(options));
	}

	#[must_use]
	pub fn for_location(mut self, location: &tg::Location) -> Self {
		self.update_options(&Update::ForLocation(location));
		self
	}

	fn update_options(&mut self, update: &Update<'_>) {
		let resource = self.executable.node.artifact.clone().map(tg::Id::from);
		update_options(&mut self.executable.options, update, resource.as_ref());
		if let Some(stdin) = &mut self.stdin {
			update_options(&mut stdin.options, update, Some(&stdin.node.clone().into()));
		}
		for value in self.args.iter_mut().chain(self.env.values_mut()) {
			let (tg::command::data::Value::String(value) | tg::command::data::Value::Value(value)) =
				value;
			update_value_options(value, update);
		}
	}

	pub fn id(&self) -> tg::Result<tg::command::Id> {
		let bytes = self.to_command_data().serialize()?;
		let id = tg::command::Id::new(&bytes);
		Ok(id)
	}

	#[must_use]
	pub fn to_command_data(&self) -> tg::command::Data {
		let data = tg::command::Data {
			args: self.args.clone(),
			cwd: self.cwd.clone(),
			env: self.env.clone(),
			executable: self.executable.node.clone(),
			host: self.host.clone(),
			stdin: self.stdin.as_ref().map(|stdin| stdin.node.clone()),
			user: self.user.clone(),
		};
		data.without_location_and_tokens()
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Referent<tg::object::Id>> {
		let mut objects = Vec::new();
		if let Some(artifact) = &self.executable.node.artifact {
			objects.push(tg::Referent::new(
				artifact.clone().into(),
				self.executable.options.clone(),
			));
		}
		for value in self.args.iter().chain(self.env.values()) {
			let (tg::command::data::Value::String(value) | tg::command::data::Value::Value(value)) =
				value;
			value.children_with_tokens(&mut objects);
		}
		if let Some(stdin) = &self.stdin {
			objects.push(stdin.clone().map(Into::into));
		}
		objects
	}

	#[must_use]
	pub fn to_spawn_arg(&self) -> tg::process::spawn::CommandArg {
		tg::process::spawn::CommandArg {
			args: self.args.clone(),
			cwd: self.cwd.clone(),
			env: self.env.clone(),
			executable: self.executable.clone(),
			host: Some(self.host.clone()),
			stdin: self.stdin.clone(),
			user: self.user.clone(),
		}
	}

	#[must_use]
	pub fn without_location_and_tokens(mut self) -> Self {
		self.args = self
			.args
			.into_iter()
			.map(tg::command::data::Value::without_location_and_tokens)
			.collect();
		self.env = self
			.env
			.into_iter()
			.map(|(key, value)| (key, value.without_location_and_tokens()))
			.collect();
		self.executable.options.clear_location_and_tokens();
		if let Some(stdin) = &mut self.stdin {
			stdin.options.clear_location_and_tokens();
		}
		self
	}
}

impl tg::Referent<tg::Either<Box<Command>, tg::command::Id>> {
	pub fn command_id(&self) -> tg::Result<tg::command::Id> {
		match &self.node {
			tg::Either::Left(command) => command.id(),
			tg::Either::Right(id) => Ok(id.clone()),
		}
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Referent<tg::object::Id>> {
		match &self.node {
			tg::Either::Left(command) => {
				let mut command = command.clone();
				command.inherit_location_and_tokens(&self.options);
				command.objects()
			},
			tg::Either::Right(id) => {
				vec![tg::Referent::new(id.clone().into(), self.options.clone())]
			},
		}
	}

	pub async fn resolve_with_handle<H>(&self, handle: &H) -> tg::Result<Command>
	where
		H: tg::Handle,
	{
		let mut command = match &self.node {
			tg::Either::Left(command) => command.as_ref().clone(),
			tg::Either::Right(id) => {
				let referent = tg::Referent::new(id.clone(), self.options.clone());
				let command = tg::Command::with_referent(referent);
				let data = command.data_with_handle(handle).await?;
				Command::with_command_data(data, &command.to_referent().options)
			},
		};
		command.inherit_location_and_tokens(&self.options);
		Ok(command)
	}
}

fn update_options(
	target: &mut tg::referent::Options,
	update: &Update<'_>,
	resource: Option<&tg::Id>,
) {
	match update {
		Update::ForLocation(location) => {
			target.location = None;
			target.tokens = target.tokens.for_location(location);
			target.tokens.normalize(resource);
		},
		Update::Inherit(source) => {
			if target.location.is_none() {
				target.location.clone_from(&source.location);
			}
			target
				.tokens
				.inherit_with_resource(&source.tokens, resource);
		},
	}
}

fn update_value_options(value: &mut tg::value::Data, update: &Update<'_>) {
	match value {
		tg::value::Data::Array(values) => {
			for value in values {
				update_value_options(value, update);
			}
		},
		tg::value::Data::Map(values) => {
			for value in values.values_mut() {
				update_value_options(value, update);
			}
		},
		tg::value::Data::Module(module) => {
			let mut children = BTreeSet::new();
			module.children(&mut children);
			let resource = children.into_iter().next().map(tg::Id::from);
			update_options(&mut module.referent.options, update, resource.as_ref());
		},
		tg::value::Data::Mutation(mutation) => match mutation {
			tg::mutation::Data::Append { values } | tg::mutation::Data::Prepend { values } => {
				for value in values {
					update_value_options(value, update);
				}
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					update_value_options(value, update);
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				update_template_options(template, update);
			},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				update_value_options(value, update);
			},
			tg::mutation::Data::Unset => {},
		},
		tg::value::Data::Object(object) => {
			update_options(
				&mut object.options,
				update,
				Some(&object.node.clone().into()),
			);
		},
		tg::value::Data::Template(template) => update_template_options(template, update),
		tg::value::Data::Bool(_)
		| tg::value::Data::Bytes(_)
		| tg::value::Data::Null
		| tg::value::Data::Number(_)
		| tg::value::Data::Placeholder(_)
		| tg::value::Data::String(_) => {},
	}
}

fn update_template_options(template: &mut tg::template::Data, update: &Update<'_>) {
	for component in &mut template.components {
		if let tg::template::data::Component::Artifact(artifact) = component {
			update_options(
				&mut artifact.options,
				update,
				Some(&artifact.node.clone().into()),
			);
		}
	}
}
