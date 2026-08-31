use {
	super::Data,
	crate::prelude::*,
	std::{collections::BTreeMap, path::PathBuf},
};

#[derive(Clone, Debug)]
pub struct Command {
	pub args: Vec<Value>,
	pub cwd: Option<PathBuf>,
	pub env: BTreeMap<String, Value>,
	pub executable: Executable,
	pub host: String,
	pub stdin: Option<tg::Blob>,
	pub user: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Value {
	String(tg::Value),
	Value(tg::Value),
}

#[derive(Clone, Debug, Default)]
pub struct Executable {
	pub artifact: Option<tg::Artifact>,
	pub path: Option<PathBuf>,
}

impl Command {
	#[must_use]
	pub fn to_data(&self) -> Data {
		let args = self.args.iter().map(Value::to_data).collect();
		let cwd = self.cwd.clone();
		let env = self
			.env
			.iter()
			.map(|(key, value)| {
				let key = key.clone();
				let value = value.to_data();
				(key, value)
			})
			.collect();
		let executable = self.executable.to_data();
		let host = self.host.clone();
		let stdin = self.stdin.as_ref().map(tg::Blob::id);
		let user = self.user.clone();
		Data {
			args,
			cwd,
			env,
			executable,
			host,
			stdin,
			user,
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let args = data
			.args
			.into_iter()
			.map(Value::try_from_data)
			.collect::<tg::Result<_>>()?;
		let cwd = data.cwd;
		let env = data
			.env
			.into_iter()
			.map(|(key, data)| Ok::<_, tg::Error>((key, Value::try_from_data(data)?)))
			.collect::<tg::Result<_>>()?;
		let executable = Executable::try_from_data(data.executable)?;
		let host = data.host;
		let stdin = data.stdin.map(tg::Blob::with_id);
		let user = data.user;
		Ok(Self {
			args,
			cwd,
			env,
			executable,
			host,
			stdin,
			user,
		})
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(self.executable.objects())
			.chain(self.args.iter().flat_map(Value::objects))
			.chain(self.env.values().flat_map(Value::objects))
			.chain(self.stdin.iter().cloned().map(Into::into))
			.collect()
	}
}

impl Value {
	#[must_use]
	pub fn into_value(self) -> tg::Value {
		match self {
			Self::String(value) | Self::Value(value) => value,
		}
	}

	#[must_use]
	pub fn to_data(&self) -> tg::command::data::Value {
		match self {
			Self::String(value) => tg::command::data::Value::String(value.to_data()),
			Self::Value(value) => tg::command::data::Value::Value(value.to_data()),
		}
	}

	pub fn try_from_data(data: tg::command::data::Value) -> tg::Result<Self> {
		let value = match data {
			tg::command::data::Value::String(value) => {
				Self::String(tg::Value::try_from_data(value)?)
			},
			tg::command::data::Value::Value(value) => Self::Value(tg::Value::try_from_data(value)?),
		};
		Ok(value)
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Object> {
		match self {
			Self::String(value) | Self::Value(value) => value.objects(),
		}
	}
}

impl Executable {
	#[must_use]
	pub fn to_data(&self) -> tg::command::data::Executable {
		let artifact = self.artifact.as_ref().map(tg::Artifact::id);
		let path = self.path.clone();
		tg::command::data::Executable { artifact, path }
	}

	pub fn try_from_data(data: tg::command::data::Executable) -> tg::Result<Self> {
		let artifact = data.artifact.map(tg::Artifact::with_id);
		let path = data.path;
		Ok(Self { artifact, path })
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Object> {
		self.artifact.iter().cloned().map(Into::into).collect()
	}
}

impl From<tg::Artifact> for Value {
	fn from(value: tg::Artifact) -> Self {
		Self::String(tg::Value::Object(value.into()))
	}
}

impl From<tg::File> for Value {
	fn from(value: tg::File) -> Self {
		let artifact = tg::Artifact::from(value);
		Self::String(tg::Value::Object(artifact.into()))
	}
}

impl From<tg::Placeholder> for Value {
	fn from(value: tg::Placeholder) -> Self {
		Self::String(tg::Value::Placeholder(value))
	}
}

impl From<String> for Value {
	fn from(value: String) -> Self {
		Self::String(tg::Value::String(value))
	}
}

impl From<&str> for Value {
	fn from(value: &str) -> Self {
		Self::String(tg::Value::String(value.to_owned()))
	}
}

impl From<tg::Template> for Value {
	fn from(value: tg::Template) -> Self {
		Self::String(tg::Value::Template(value))
	}
}

impl From<tg::File> for Executable {
	fn from(value: tg::File) -> Self {
		Self {
			artifact: Some(value.into()),
			path: None,
		}
	}
}
