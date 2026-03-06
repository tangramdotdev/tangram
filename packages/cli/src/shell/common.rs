use {
	crate::Cli,
	futures::FutureExt as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		fmt::Write as _,
		path::{Path, PathBuf},
		process::Stdio,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct State {
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub current: BTreeMap<String, Option<String>>,

	#[serde(default)]
	pub directory: bool,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub previous: BTreeMap<String, Option<String>>,

	pub reference: tg::Reference,
}

#[derive(Clone, Debug)]
pub enum Mutation {
	Set { key: String, value: String },
	Unset { key: String },
}

#[derive(Clone, Debug)]
pub struct DeactivateShellOutput {
	pub mutations: Vec<Mutation>,
	pub preserved: Vec<String>,
	pub reference: tg::Reference,
}

impl Cli {
	pub(super) fn get_shell_directory(
		&self,
		path: &Path,
	) -> tg::Result<Option<(PathBuf, crate::config::ShellDirectory)>> {
		let config = self.read_config()?;
		let Some(shell) = config.shell else {
			return Ok(None);
		};
		if shell.directories.is_empty() {
			return Ok(None);
		}
		let mut directories = shell
			.directories
			.iter()
			.map(|(directory, value)| (PathBuf::from(directory), value))
			.collect::<Vec<_>>();
		directories.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
		let directory = directories
			.into_iter()
			.rev()
			.find(|(directory, _)| path.starts_with(directory))
			.map(|(directory, value)| (directory, value.clone()));
		let Some((directory, directory_value)) = directory else {
			return Ok(None);
		};
		Ok(Some((directory, directory_value)))
	}

	pub(super) fn shell_state_path() -> tg::Result<PathBuf> {
		let path = std::env::var("XDG_STATE_HOME")
			.map(PathBuf::from)
			.or_else(|_| {
				Ok::<_, tg::Error>(
					PathBuf::from(std::env::var("HOME").unwrap()).join(".local/state"),
				)
			})
			.map_err(|source| tg::error!(!source, "failed to get the state directory"))?
			.join("tangram/shell/state");
		std::fs::create_dir_all(&path)
			.map_err(|source| tg::error!(!source, "failed to create the state directory"))?;
		let bytes = rand::random::<[u8; 10]>();
		let bytes = tg::id::ENCODING.encode(&bytes);
		let name = format!("{bytes}.json");
		let path = path.join(name);
		Ok(path)
	}

	pub(super) fn load_shell_state(path: impl AsRef<Path>) -> tg::Result<Option<State>> {
		let path = path.as_ref();
		let state = match std::fs::read_to_string(path) {
			Ok(state) => state,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(source) => {
				return Err(tg::error!(
					!source,
					path = %path.display(),
					"failed to read the shell state file"
				));
			},
		};
		let state = serde_json::from_str(&state).map_err(
			|source| tg::error!(!source, path = %path.display(), "failed to deserialize the shell state"),
		)?;
		Ok(Some(state))
	}

	pub(super) fn write_shell_state(path: &Path, state: &State) -> tg::Result<()> {
		let json = serde_json::to_vec_pretty(state)
			.map_err(|source| tg::error!(!source, "failed to serialize the shell state"))?;
		let temporary = path.with_extension("json.tmp");
		std::fs::write(&temporary, json)
			.map_err(|source| tg::error!(!source, "failed to write the shell state file"))?;
		std::fs::rename(&temporary, path)
			.map_err(|source| tg::error!(!source, "failed to commit the shell state file"))?;
		Ok(())
	}

	pub(super) fn deactivate_shell() -> tg::Result<Option<DeactivateShellOutput>> {
		let shell_state_path = std::env::var("TANGRAM_SHELL_STATE").ok();
		let Some(path) = shell_state_path.as_deref() else {
			return Ok(None);
		};
		let Some(state) = Self::load_shell_state(path)? else {
			return Ok(None);
		};
		let current = std::env::vars().collect::<BTreeMap<_, _>>();
		let mut mutations = Vec::new();
		let mut preserved = Vec::new();
		for (key, state_current) in &state.current {
			let current = current.get(key.as_str()).cloned();
			if current != *state_current {
				preserved.push(key.clone());
				continue;
			}
			let previous = state.previous.get(key.as_str()).cloned().flatten();
			if current == previous {
				continue;
			}
			match previous {
				Some(value) => {
					mutations.push(Mutation::Set {
						key: key.clone(),
						value,
					});
				},
				None => {
					mutations.push(Mutation::Unset { key: key.clone() });
				},
			}
		}
		let deactivate = DeactivateShellOutput {
			mutations,
			preserved,
			reference: state.reference,
		};
		Ok(Some(deactivate))
	}

	pub(super) fn print_shell_preserved_variable_messages(keys: &[String]) {
		if keys.is_empty() {
			return;
		}
		Self::print_warning_message(&format!(
			"preserved environment variables: {}.",
			keys.join(", ")
		));
	}

	pub(super) fn create_shell_mutations(
		current: &BTreeMap<String, String>,
		target: &BTreeMap<String, String>,
	) -> (
		Vec<Mutation>,
		BTreeMap<String, Option<String>>,
		BTreeMap<String, Option<String>>,
	) {
		let keys = current
			.keys()
			.chain(target.keys())
			.cloned()
			.collect::<BTreeSet<_>>();
		let mut mutations = Vec::new();
		let mut previous = BTreeMap::new();
		let mut next = BTreeMap::new();
		for key in keys {
			if ignored(&key) {
				continue;
			}
			let key_previous = current.get(&key).cloned();
			let key_current = target.get(&key).cloned();
			if key_previous == key_current {
				continue;
			}
			match key_current.clone() {
				Some(value) => {
					mutations.push(Mutation::Set {
						key: key.clone(),
						value,
					});
				},
				None => {
					mutations.push(Mutation::Unset { key: key.clone() });
				},
			}
			previous.insert(key.clone(), key_previous);
			next.insert(key, key_current);
		}
		(mutations, previous, next)
	}

	pub(super) fn create_shell_code(shell: super::Kind, mutations: &[Mutation]) -> String {
		let mut output = String::new();
		for mutation in mutations {
			match mutation {
				Mutation::Set { key, value } => {
					let value = quote(shell, value);
					match shell {
						super::Kind::Bash | super::Kind::Zsh => {
							writeln!(output, "export {key}={value}").unwrap();
						},
						super::Kind::Fish => {
							writeln!(output, "set -gx {key} {value}").unwrap();
						},
						super::Kind::Nu => {
							writeln!(output, "load-env {{ {key}: {value} }}").unwrap();
						},
					}
				},
				Mutation::Unset { key } => match shell {
					super::Kind::Bash | super::Kind::Zsh => {
						writeln!(output, "unset {key}").unwrap();
					},
					super::Kind::Fish => {
						writeln!(output, "set -e {key}").unwrap();
					},
					super::Kind::Nu => {
						writeln!(output, "hide-env {key}").unwrap();
					},
				},
			}
		}
		output
	}

	pub(super) fn create_shell_state_code(shell: super::Kind, path: Option<&Path>) -> String {
		match path {
			Some(path) => {
				let path = path.to_str().unwrap();
				let path = quote(shell, path);
				match shell {
					super::Kind::Bash | super::Kind::Zsh => {
						format!("export TANGRAM_SHELL_STATE={path}\n")
					},
					super::Kind::Fish => {
						format!("set -gx TANGRAM_SHELL_STATE {path}\n")
					},
					super::Kind::Nu => {
						format!("load-env {{ TANGRAM_SHELL_STATE: {path} }}\n")
					},
				}
			},
			None => match shell {
				super::Kind::Bash | super::Kind::Zsh => "unset TANGRAM_SHELL_STATE\n".to_owned(),
				super::Kind::Fish => "set -e TANGRAM_SHELL_STATE\n".to_owned(),
				super::Kind::Nu => "hide-env TANGRAM_SHELL_STATE\n".to_owned(),
			},
		}
	}

	pub(super) fn apply_shell_mutations(
		env: &mut BTreeMap<String, String>,
		mutations: &[Mutation],
	) {
		for mutation in mutations {
			match mutation {
				Mutation::Set { key, value } => {
					env.insert(key.clone(), value.clone());
				},
				Mutation::Unset { key } => {
					env.remove(key);
				},
			}
		}
	}

	pub(super) async fn build_shell_executable(
		&mut self,
		reference: &tg::Reference,
	) -> tg::Result<PathBuf> {
		let handle = self.handle().await?;
		let options = crate::process::spawn::Options {
			sandbox: crate::process::spawn::Sandbox::new(Some(true)),
			..Default::default()
		};
		let process = self
			.spawn(
				options,
				reference.clone(),
				Vec::new(),
				None,
				tg::process::Stdio::default(),
				tg::process::Stdio::default(),
				tg::process::Stdio::default(),
			)
			.boxed()
			.await?;

		// Wait for the process to finish while showing the viewer if enabled.
		let wait = {
			let view_task = {
				let handle = handle.clone();
				let root = process.clone().map(crate::viewer::Item::Process);
				let task = Task::spawn_blocking(move |stop| {
					let local_set = tokio::task::LocalSet::new();
					let runtime = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
						.map_err(|source| {
							tg::error!(!source, "failed to create the tokio runtime")
						})?;
					local_set.block_on(&runtime, async move {
						let viewer_options = crate::viewer::Options {
							collapse_process_children: true,
							depth: None,
							expand_objects: false,
							expand_packages: false,
							expand_processes: true,
							expand_metadata: false,
							expand_tags: false,
							expand_values: false,
							show_process_commands: false,
						};
						let mut viewer = crate::viewer::Viewer::new(&handle, root, viewer_options);
						viewer.run_inline(stop, false).await?;
						Ok::<_, tg::Error>(())
					})
				});
				Some(task)
			};

			let cancel_task = tokio::spawn({
				let handle = handle.clone();
				let process = process.clone();
				async move {
					tokio::signal::ctrl_c().await.unwrap();
					tokio::spawn(async move {
						process
							.item()
							.cancel(&handle)
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to cancel the process");
							})
							.ok();
					});
					tokio::signal::ctrl_c().await.unwrap();
					std::process::exit(130);
				}
			});

			let arg = tg::process::wait::Arg {
				token: process.item().token().cloned(),
				..Default::default()
			};
			let result = process.item().wait(&handle, arg).await;
			cancel_task.abort();

			if let Some(view_task) = view_task {
				view_task.stop();
				match view_task.wait().await {
					Ok(Ok(())) => {},
					Ok(Err(error)) => {
						tracing::warn!(?error, "failed to render the process viewer");
						Self::print_warning_message("failed to render the process viewer");
					},
					Err(error) => {
						tracing::warn!(?error, "failed to join the process viewer task");
						Self::print_warning_message("failed to render the process viewer");
					},
				}
			}

			result?
		};

		if let Some(error) = wait.error {
			let error = match error.to_data_or_id() {
				tg::Either::Left(data) => {
					let object = tg::error::Object::try_from_data(data)
						.map_err(|source| tg::error!(!source, "invalid error"))?;
					tg::Either::Left(Box::new(object))
				},
				tg::Either::Right(id) => tg::Either::Right(Box::new(tg::Error::with_id(id))),
			};
			let error = tg::Error::with_object(tg::error::Object {
				message: Some("the process failed".to_owned()),
				source: Some(process.clone().map(|_| error)),
				values: [("id".to_owned(), process.item().id().to_string())].into(),
				..Default::default()
			});
			return Err(error);
		}

		if wait.exit > 0 && wait.exit < 128 {
			return Err(tg::error!("the process exited with code {}", wait.exit));
		}
		if wait.exit >= 128 {
			return Err(tg::error!(
				"the process exited with signal {}",
				wait.exit - 128
			));
		}

		let output = wait.output.unwrap_or(tg::Value::Null);
		let artifact: tg::Artifact = output
			.try_into()
			.map_err(|_| tg::error!("expected an artifact"))?;
		let artifact = artifact.id();
		let arg = tg::checkout::Arg {
			artifact: artifact.clone(),
			dependencies: true,
			extension: None,
			force: true,
			lock: None,
			path: None,
		};
		let stream = handle
			.checkout(arg)
			.await
			.map_err(|source| tg::error!(!source, %artifact, "failed to check out the artifact"))?;
		let tg::checkout::Output { path, .. } = self
			.render_progress_stream(stream)
			.await
			.map_err(|source| tg::error!(!source, %artifact, "failed to check out the artifact"))?;

		Ok(path)
	}

	pub(super) async fn run_shell_executable(
		&mut self,
		path: &Path,
	) -> tg::Result<BTreeMap<String, String>> {
		let output = tokio::process::Command::new(path)
			.arg("-0")
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::inherit())
			.output()
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					path = %path.display(),
					"failed to spawn the process"
				)
			})?;
		if !output.status.success() {
			let status = output.status;
			let code = status
				.code()
				.map_or_else(|| "<none>".to_owned(), |code| code.to_string());
			return Err(tg::error!(
				%code,
				path = %path.display(),
				"the shell environment process exited unsuccessfully"
			));
		}
		let mut env = BTreeMap::new();
		for record in output.stdout.split(|byte| *byte == 0) {
			if record.is_empty() {
				continue;
			}
			let Some(index) = record.iter().position(|byte| *byte == b'=') else {
				continue;
			};
			let key = std::str::from_utf8(&record[..index])
				.map_err(|source| tg::error!(!source, "failed to parse an env key"))?;
			let value = std::str::from_utf8(&record[index + 1..])
				.map_err(|source| tg::error!(!source, "failed to parse an env value"))?;
			if ignored(key) {
				continue;
			}
			env.insert(key.to_owned(), value.to_owned());
		}
		Ok(env)
	}
}

fn ignored(key: &str) -> bool {
	matches!(
		key,
		"_" | "OLDPWD"
			| "PPID" | "PWD"
			| "SHLVL" | "TANGRAM_SHELL_STATE"
			| "TANGRAM_HOST"
			| "TANGRAM_OUTPUT"
			| "TANGRAM_PROCESS"
			| "TANGRAM_URL"
	)
}

fn quote(shell: super::Kind, value: &str) -> String {
	match shell {
		super::Kind::Bash | super::Kind::Fish | super::Kind::Zsh => {
			let value = value.replace('\'', "'\"'\"'");
			format!("'{value}'")
		},
		super::Kind::Nu => {
			let value = value.replace('\'', "''");
			format!("'{value}'")
		},
	}
}
