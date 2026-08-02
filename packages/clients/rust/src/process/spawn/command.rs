use {
	crate::prelude::*,
	std::{collections::BTreeMap, path::PathBuf},
};

#[derive(Clone, Debug)]
pub struct CommandArg {
	pub args: Vec<tg::command::Value>,
	pub cwd: Option<PathBuf>,
	pub env: BTreeMap<String, tg::command::Value>,
	pub executable: tg::command::Executable,
	pub host: Option<String>,
	pub stdin: Option<tg::Blob>,
	pub user: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Data {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	args: Vec<tg::command::data::Value>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	cwd: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	env: BTreeMap<String, tg::command::data::Value>,

	executable: Executable,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	host: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	stdin: Option<tg::Referent<tg::blob::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	user: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Executable {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	artifact: Option<tg::Referent<tg::artifact::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	path: Option<PathBuf>,
}

impl CommandArg {
	#[must_use]
	pub fn with_object(object: &tg::command::Object) -> Self {
		Self {
			args: object.args.clone(),
			cwd: object.cwd.clone(),
			env: object.env.clone(),
			executable: object.executable.clone(),
			host: Some(object.host.clone()),
			stdin: object.stdin.clone(),
			user: object.user.clone(),
		}
	}

	#[must_use]
	pub fn into_object(self, host: String) -> tg::command::Object {
		tg::command::Object {
			args: self.args,
			cwd: self.cwd,
			env: self.env,
			executable: self.executable,
			host,
			stdin: self.stdin,
			user: self.user,
		}
	}

	#[must_use]
	pub fn objects(&self) -> Vec<tg::Object> {
		std::iter::empty()
			.chain(self.executable.objects())
			.chain(self.args.iter().flat_map(tg::command::Value::objects))
			.chain(self.env.values().flat_map(tg::command::Value::objects))
			.chain(self.stdin.iter().cloned().map(Into::into))
			.collect()
	}
}

impl serde::Serialize for CommandArg {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		let artifact = self.executable.artifact.as_ref().map(|artifact| {
			tg::Referent::with_item_and_token(artifact.id(), artifact.state().token())
		});
		let executable = Executable {
			artifact,
			path: self.executable.path.clone(),
		};
		let stdin = self
			.stdin
			.as_ref()
			.map(|blob| tg::Referent::with_item_and_token(blob.id(), blob.state().token()));
		let data = Data {
			args: self.args.iter().map(tg::command::Value::to_data).collect(),
			cwd: self.cwd.clone(),
			env: self
				.env
				.iter()
				.map(|(key, value)| (key.clone(), value.to_data()))
				.collect(),
			executable,
			host: self.host.clone(),
			stdin,
			user: self.user.clone(),
		};
		data.serialize(serializer)
	}
}

impl<'de> serde::Deserialize<'de> for CommandArg {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		let data = Data::deserialize(deserializer)?;
		let args = data
			.args
			.into_iter()
			.map(tg::command::Value::try_from_data)
			.collect::<tg::Result<_>>()
			.map_err(serde::de::Error::custom)?;
		let env = data
			.env
			.into_iter()
			.map(|(key, value)| Ok((key, tg::command::Value::try_from_data(value)?)))
			.collect::<tg::Result<_>>()
			.map_err(serde::de::Error::custom)?;
		let executable = tg::command::Executable {
			artifact: data.executable.artifact.map(tg::Artifact::with_referent),
			path: data.executable.path,
		};
		let stdin = data.stdin.map(tg::Blob::with_referent);
		Ok(Self {
			args,
			cwd: data.cwd,
			env,
			executable,
			host: data.host,
			stdin,
			user: data.user,
		})
	}
}
