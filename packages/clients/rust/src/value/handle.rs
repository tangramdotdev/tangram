use {
	super::{
		Data, parse,
		print::{self, Printer},
	},
	crate::prelude::*,
	bytes::Bytes,
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	std::collections::{BTreeMap, BTreeSet},
};

/// A value.
#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
	serde::Deserialize,
)]
#[serde(try_from = "Data")]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Value {
	/// A null value.
	Null,

	/// A bool value.
	Bool(bool),

	/// A number value.
	Number(f64),

	/// A string value.
	String(String),

	/// An array value.
	Array(Array),

	/// A map value.
	Map(Map),

	/// An object value.
	Object(tg::object::Handle),

	/// A bytes value.
	Bytes(Bytes),

	/// A mutation value.
	Mutation(tg::Mutation),

	/// A module value.
	Module(tg::Module),

	/// A template value.
	Template(tg::Template),

	/// A placeholder value.
	Placeholder(tg::Placeholder),
}

pub type Array = Vec<Value>;

pub type Map = BTreeMap<String, Value>;

impl Value {
	pub fn objects(&self) -> Vec<tg::object::Handle> {
		match self {
			Self::Array(array) => array.iter().flat_map(Self::objects).collect(),
			Self::Map(map) => map.values().flat_map(Self::objects).collect(),
			Self::Object(object) => vec![object.clone()],
			Self::Template(template) => template.objects(),
			Self::Mutation(mutation) => mutation.objects(),
			Self::Module(module) => module.children(),
			_ => vec![],
		}
	}

	pub(crate) fn inherit_tokens(&self, tokens: &tg::authorization::Tokens) {
		for object in self.objects() {
			object.inherit_tokens(tokens);
		}
	}

	pub(crate) fn inherit_location(&self, location: Option<&tg::Location>) {
		for object in self.objects() {
			object.inherit_location(location);
		}
	}

	pub async fn store(&self) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.store_with_handle(handle).await
	}

	pub async fn store_with_location(&self, location: Option<tg::Location>) -> tg::Result<()> {
		let handle = tg::handle()?;
		self.store_with_location_with_handle(handle, location).await
	}

	pub async fn store_with_handle<H>(&self, handle: &H) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		self.store_with_location_with_handle(handle, None).await
	}

	pub async fn store_with_location_with_handle<H>(
		&self,
		handle: &H,
		location: Option<tg::Location>,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		loop {
			// Collect all unstored states with children before parents.
			let mut pending = Vec::new();
			let mut states = Vec::new();
			let mut stack = self
				.objects()
				.into_iter()
				.map(|object| (object, false))
				.collect::<Vec<_>>();
			let mut visited = BTreeSet::new();
			while let Some((object, expanded)) = stack.pop() {
				let state = object.state();
				if expanded {
					states.push(state);
					continue;
				}
				if !visited.insert(state.identity()) || state.stored() {
					continue;
				}
				if let Some(task) = state.store_task() {
					if !pending
						.iter()
						.any(|pending: &tg::object::state::StoreTask| pending.id() == task.id())
					{
						pending.push(task);
					}
					continue;
				}
				stack.push((object, true));
				if let Some(object) = state.object() {
					stack.extend(object.children().into_iter().map(|object| (object, false)));
				}
			}

			// Wait for overlapping store tasks and plan the batch again.
			if !pending.is_empty() {
				pending
					.into_iter()
					.map(|task| async move { task.wait().await })
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
				continue;
			}
			if states.is_empty() {
				return Ok(());
			}

			// Claim the states and start the store task.
			let handle = handle.clone();
			let location = location.clone();
			let status = tg::object::State::start_store_task(states, move |states| {
				tg::object::state::StoreTask::spawn(states, move |states| async move {
					Self::store_task(handle, location, states).await
				})
			});
			match status {
				tg::object::state::StoreTaskStatus::Complete => return Ok(()),
				tg::object::state::StoreTaskStatus::Pending(tasks) => {
					tasks
						.into_iter()
						.map(|task| async move { task.wait().await })
						.collect::<FuturesUnordered<_>>()
						.try_collect::<()>()
						.await?;
				},
				tg::object::state::StoreTaskStatus::Started(task) => return task.wait().await,
			}
		}
	}

	async fn store_task<H>(
		handle: H,
		location: Option<tg::Location>,
		states: Vec<tg::object::State>,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		// Create the batch.
		let mut objects = Vec::with_capacity(states.len());
		let mut state_group_indices = BTreeMap::<tg::object::Id, usize>::new();
		let mut state_groups = Vec::<Vec<tg::object::State>>::new();
		for state in &states {
			let object = state
				.object()
				.ok_or_else(|| tg::error!("expected the object to be loaded"))?;
			let data = object.to_data().without_location_and_tokens();
			let bytes = data
				.serialize()
				.map_err(|error| tg::error!(!error, "failed to serialize the data"))?;
			let id = tg::object::Id::new(data.kind(), &bytes);
			state.set_id(id.clone());
			let children = object
				.children()
				.iter()
				.map(Self::object_referent)
				.collect();
			let batch_object = tg::object::batch::Object {
				bytes,
				children,
				id: id.clone(),
			};
			let state_group_index = if let Some(&state_group_index) = state_group_indices.get(&id) {
				objects[state_group_index] = batch_object;
				state_group_index
			} else {
				let state_group_index = state_groups.len();
				objects.push(batch_object);
				state_group_indices.insert(id, state_group_index);
				state_groups.push(Vec::new());
				state_group_index
			};
			state_groups[state_group_index].push(state.clone());
		}

		// Store the batch.
		let arg = tg::object::batch::Arg {
			location: location.map(Into::into),
			objects,
		};
		let output = handle.post_object_batch(arg).await?;

		// Update the states.
		Self::apply_object_batch_output(&state_groups, output)?;

		Ok(())
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Null => Data::Null,
			Self::Bool(bool) => Data::Bool(*bool),
			Self::Number(number) => Data::Number(*number),
			Self::String(string) => Data::String(string.clone()),
			Self::Array(array) => Data::Array(array.iter().map(Value::to_data).collect()),
			Self::Map(map) => Data::Map(
				map.iter()
					.map(|(key, value)| (key.clone(), value.to_data()))
					.collect(),
			),
			Self::Object(object) => {
				let id = object.id();
				let location = object.state().location();
				let tokens = object.state().tokens();
				let options = tg::referent::Options {
					location,
					tokens,
					..tg::referent::Options::default()
				};
				Data::Object(tg::Referent::new(id, options))
			},
			Self::Bytes(bytes) => Data::Bytes(bytes.clone()),
			Self::Mutation(mutation) => Data::Mutation(mutation.to_data()),
			Self::Module(module) => Data::Module(module.to_data()),
			Self::Template(template) => Data::Template(template.to_data()),
			Self::Placeholder(placeholder) => Data::Placeholder(placeholder.to_data()),
		}
	}

	fn apply_object_batch_output(
		state_groups: &[Vec<tg::object::State>],
		output: tg::object::batch::Output,
	) -> tg::Result<()> {
		if state_groups.len() != output.objects.len() {
			return Err(tg::error!("invalid object batch output"));
		}
		for (states, object) in state_groups.iter().zip(&output.objects) {
			if states.is_empty() {
				return Err(tg::error!("invalid object batch output"));
			}
			for state in states {
				if state.try_get_id().as_ref() != Some(&object.node) {
					return Err(tg::error!("invalid object batch output"));
				}
			}
		}
		for (states, object) in state_groups.iter().zip(output.objects) {
			for state in states {
				state.finish_store(object.clone())?;
			}
		}
		Ok(())
	}

	fn object_referent(object: &tg::Object) -> tg::Referent<tg::object::Id> {
		object.to_referent()
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let value = match data {
			Data::Null => Self::Null,
			Data::Bool(bool) => Self::Bool(bool),
			Data::Number(number) => Self::Number(number),
			Data::String(string) => Self::String(string),
			Data::Array(array) => Self::Array(
				array
					.into_iter()
					.map(Self::try_from_data)
					.collect::<tg::Result<_>>()?,
			),
			Data::Map(map) => Self::Map(
				map.into_iter()
					.map(|(key, value)| Ok::<_, tg::Error>((key, Self::try_from_data(value)?)))
					.collect::<tg::Result<_>>()?,
			),
			Data::Object(object) => Self::Object(tg::object::Handle::with_referent(object)),
			Data::Bytes(bytes) => Self::Bytes(bytes),
			Data::Mutation(mutation) => Self::Mutation(tg::Mutation::try_from_data(mutation)?),
			Data::Module(module) => Self::Module(tg::Module::try_from(module)?),
			Data::Template(template) => Self::Template(tg::Template::try_from_data(template)?),
			Data::Placeholder(placeholder) => {
				Self::Placeholder(tg::Placeholder::try_from_data(placeholder)?)
			},
		};
		Ok(value)
	}

	pub async fn children(&self) -> tg::Result<Vec<Self>> {
		let handle = tg::handle()?;
		self.children_with_handle(handle).await
	}

	pub async fn children_with_handle<H>(&self, handle: &H) -> tg::Result<Vec<Self>>
	where
		H: tg::Handle,
	{
		self.children_with_arg_with_handle(handle, tg::object::get::Arg::default())
			.await
	}

	pub async fn children_with_arg(&self, arg: tg::object::get::Arg) -> tg::Result<Vec<Self>> {
		let handle = tg::handle()?;
		self.children_with_arg_with_handle(handle, arg).await
	}

	pub async fn children_with_arg_with_handle<H>(
		&self,
		handle: &H,
		arg: tg::object::get::Arg,
	) -> tg::Result<Vec<Self>>
	where
		H: tg::Handle,
	{
		let mut children = Vec::new();
		match self {
			Self::Object(object) => {
				for child in object.children_with_arg_with_handle(handle, arg).await? {
					children.push(tg::Value::Object(child));
				}
			},
			Self::Array(array) => {
				for child in array {
					children.push(child.clone());
				}
			},
			Self::Map(map) => {
				for child in map.values() {
					children.push(child.clone());
				}
			},
			Self::Template(template) => {
				for object in template.objects() {
					children.push(tg::Value::Object(object));
				}
			},
			Self::Mutation(mutation) => {
				for object in mutation.objects() {
					children.push(tg::Value::Object(object));
				}
			},
			Self::Module(module) => {
				for object in module.children() {
					children.push(tg::Value::Object(object));
				}
			},
			_ => (),
		}
		Ok(children)
	}

	pub fn print(&self, options: print::Options) -> String {
		let mut string = String::new();
		let mut printer = Printer::new(&mut string, options);
		printer.value(self).unwrap();
		string
	}

	pub fn is_blob(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_blob())
	}

	pub fn is_artifact(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_artifact())
	}

	pub fn is_directory(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_directory())
	}

	pub fn is_file(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_file())
	}

	pub fn is_symlink(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_symlink())
	}

	pub fn is_graph(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_graph())
	}

	pub fn is_command(&self) -> bool {
		matches!(self, Self::Object(object) if object.is_command())
	}
}

impl std::fmt::Display for Value {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = Printer::new(f, tg::value::print::Options::default());
		printer.value(self)?;
		Ok(())
	}
}

impl std::str::FromStr for Value {
	type Err = tg::Error;

	fn from_str(input: &str) -> Result<Self, Self::Err> {
		parse::parse(input)
	}
}

impl TryFrom<Data> for Value {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		Self::try_from_data(data)
	}
}

impl From<&str> for Value {
	fn from(value: &str) -> Self {
		value.to_owned().into()
	}
}

impl<T> From<Option<T>> for Value
where
	T: Into<Value>,
{
	fn from(value: Option<T>) -> Self {
		match value {
			Some(value) => value.into(),
			None => Self::Null,
		}
	}
}

impl From<tg::Blob> for Value {
	fn from(value: tg::Blob) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Blob {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Directory> for Value {
	fn from(value: tg::Directory) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Directory {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::File> for Value {
	fn from(value: tg::File) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::File {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Symlink> for Value {
	fn from(value: tg::Symlink) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Symlink {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Graph> for Value {
	fn from(value: tg::Graph) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Graph {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Command> for Value {
	fn from(value: tg::Command) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Command {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<tg::Error> for Value {
	fn from(value: tg::Error) -> Self {
		tg::Object::from(value).into()
	}
}

impl TryFrom<Value> for tg::Error {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		tg::Object::try_from(value)
			.map_err(|_| tg::error!("invalid value"))?
			.try_into()
			.map_err(|_| tg::error!("invalid value"))
	}
}

impl From<serde_json::Value> for Value {
	fn from(value: serde_json::Value) -> Self {
		match value {
			serde_json::Value::Null => Self::Null,
			serde_json::Value::Bool(value) => Self::Bool(value),
			serde_json::Value::Number(value) => Self::Number(value.as_f64().unwrap()),
			serde_json::Value::String(value) => Self::String(value),
			serde_json::Value::Array(value) => {
				Self::Array(value.into_iter().map(Into::into).collect())
			},
			serde_json::Value::Object(value) => Self::Map(
				value
					.into_iter()
					.map(|(key, value)| (key, value.into()))
					.collect(),
			),
		}
	}
}

impl TryFrom<Value> for serde_json::Value {
	type Error = tg::Error;

	fn try_from(value: Value) -> Result<Self, Self::Error> {
		match value {
			Value::Null => Ok(Self::Null),
			Value::Bool(value) => Ok(Self::Bool(value)),
			Value::Number(value) => Ok(Self::Number(serde_json::Number::from_f64(value).unwrap())),
			Value::String(value) => Ok(Self::String(value)),
			Value::Array(value) => Ok(Self::Array(
				value
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<_>>()?,
			)),
			Value::Map(value) => Ok(Self::Object(
				value
					.into_iter()
					.map(|(key, value)| Ok((key, value.try_into()?)))
					.collect::<tg::Result<_>>()?,
			)),
			_ => Err(tg::error!("invalid value")),
		}
	}
}

impl<L, R> From<tg::Either<L, R>> for Value
where
	L: Into<Value>,
	R: Into<Value>,
{
	fn from(value: tg::Either<L, R>) -> Self {
		match value {
			tg::Either::Left(value) => value.into(),
			tg::Either::Right(value) => value.into(),
		}
	}
}
