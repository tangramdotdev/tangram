use {
	std::{collections::BTreeMap, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Command {
	pub args: Vec<String>,
	pub cwd: PathBuf,
	pub env: BTreeMap<String, String>,
}

impl Command {
	pub(crate) fn command(&self) -> tg::Result<std::process::Command> {
		let executable = self
			.args
			.first()
			.ok_or_else(|| tg::error!("a command is required"))?;
		let mut command = std::process::Command::new(executable);
		command
			.args(&self.args[1..])
			.current_dir(&self.cwd)
			.env_clear()
			.envs(&self.env);
		Ok(command)
	}

	/// Capture the explicit command settings, defaulting to an empty environment and the root working directory.
	pub fn with_command(command: &std::process::Command) -> tg::Result<Self> {
		// Encode the arguments.
		let args = std::iter::once(command.get_program())
			.chain(command.get_args())
			.map(|arg| {
				arg.to_str()
					.map(str::to_owned)
					.ok_or_else(|| tg::error!("launch arguments must be valid UTF-8"))
			})
			.collect::<tg::Result<_>>()?;

		// Encode the environment.
		let mut env = BTreeMap::new();
		for (key, value) in command.get_envs() {
			let key = key
				.to_str()
				.ok_or_else(|| tg::error!("environment names must be valid UTF-8"))?;
			if let Some(value) = value {
				let value = value
					.to_str()
					.ok_or_else(|| tg::error!("environment values must be valid UTF-8"))?;
				env.insert(key.to_owned(), value.to_owned());
			} else {
				env.remove(key);
			}
		}

		// Resolve the working directory.
		let cwd = command
			.get_current_dir()
			.map_or_else(|| "/".into(), PathBuf::from);

		Ok(Self { args, cwd, env })
	}
}

impl Default for Command {
	fn default() -> Self {
		Self {
			args: Vec::new(),
			cwd: "/".into(),
			env: BTreeMap::new(),
		}
	}
}
