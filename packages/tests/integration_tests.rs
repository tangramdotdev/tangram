use std::{
	collections::HashMap,
	fs::{read_to_string, File},
	io::{BufReader, Read},
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	sync::LazyLock,
	time::{Duration, Instant},
};
use tempfile::TempDir;

/// Get the path to a test package by name.
fn test_package_path(name: &str) -> Option<PathBuf> {
	let cargo_manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
	let package_path = PathBuf::from(cargo_manifest_dir)
		.join("packages")
		.join(name);
	if std::fs::exists(&package_path).unwrap_or(false) {
		Some(package_path)
	} else {
		None
	}
}

/// The path to the `tg` executable in the correct target directory.
static TG_EXECUTABLE: LazyLock<PathBuf> = LazyLock::new(|| {
	let workspace_dir =
		std::env::var("CARGO_WORKSPACE_DIR").expect("could not read CARGO_WORKSPACE_DIR");
	let target_dir = PathBuf::from(workspace_dir).join("target");
	let profile = if cfg!(debug_assertions) {
		"debug"
	} else {
		"release"
	};

	let binary_name = "tg".to_string();

	let target = std::env::var("TARGET").unwrap_or_else(|_| {
		// If TARGET is not set, get it from rustc
		let output = Command::new("rustc")
			.args(&["-vV"])
			.output()
			.ok()
			.expect("could not invoke rustc");
		String::from_utf8(output.stdout)
			.ok()
			.expect("could not produce utf8 string from rustc output")
			.lines()
			.find(|l| l.starts_with("host: "))
			.map(|l| l.trim_start_matches("host: ").to_string())
			.expect("could not find host from rustc output")
	});

	let possible_paths = vec![
		target_dir.join(&target).join(profile).join(&binary_name),
		target_dir.join(profile).join(&binary_name),
	];

	let mut ret = None;
	for path in possible_paths {
		if path.exists() {
			ret = Some(path);
			break;
		}
	}

	ret.expect("could not locate the tg executable in the target directory")
});

/// Manage multiple Tangram server instances for integration testing.
struct Harness {
	/// Containing directory to use for each server configuration and log storage.
	directory: TempDir, // Should we use $CARGO_TARGET_TMPDIR instead?
	/// Each instance is associated with a string name.
	instances: HashMap<String, ProcessInstance>,
}

/// A single running process.
struct ProcessInstance {
	/// The path to use for any files needed by this process.
	directory_path: PathBuf,
	/// A handle to the running process.
	process: Child,
}

impl Harness {
	/// Instantiate a new [`Harness`].
	fn new() -> std::io::Result<Self> {
		let directory = TempDir::new()?;

		let harness = Harness {
			directory,
			instances: HashMap::new(),
		};
		Ok(harness)
	}

	/// Create a [`std::process::Command`] to invoke a specific `tg` instance.
	fn tg(&self, name: &str) -> Option<Command> {
		if let Some(instance) = self.instances.get(name) {
			let command = instance.command();
			Some(command)
		} else {
			None
		}
	}

	/// Get a reference to an instance to query.
	fn get_instance(&self, name: &str) -> Option<&ProcessInstance> {
		self.instances.get(name)
	}

	/// Get the logs for an instance. Returns (stdout, stderr) as Strings.
	fn get_logs(&self, name: &str) -> std::io::Result<Option<(String, String)>> {
		let logs = if let Some(instance) = self.get_instance(name) {
			Some((instance.stdout()?, instance.stderr()?))
		} else {
			None
		};
		Ok(logs)
	}

	/// Create a new server instance with the given name and configuration.
	fn start_instance(&mut self, name: &str, config_json: &str) -> std::io::Result<()> {
		// Create a directory for the instance.
		let directory_path = self.directory.path().join(name);
		std::fs::create_dir_all(&directory_path)?;

		// Create the config in the tempdir.
		let config_path = directory_path.join("config.json");
		std::fs::write(&config_path, config_json)?;

		// Create a path for the server files.
		let server_path = directory_path.join(".tangram");

		// Create log files for stdout and stderr.
		let stdout_log = directory_path.join("stdout.log");
		let stderr_log = directory_path.join("stderr.log");

		let stdout_file = File::create(&stdout_log)?;
		let stderr_file = File::create(&stderr_log)?;

		let process = Command::new(&*TG_EXECUTABLE)
			.arg("--config")
			.arg(&config_path)
			.arg("--path")
			.arg(&server_path)
			.arg("serve")
			.stdout(Stdio::from(stdout_file))
			.stderr(Stdio::from(stderr_file))
			.spawn()?;

		// Wait for the server to initialize by checking for a specific log message
		const INIT_TIMEOUT: Duration = Duration::from_secs(1);
		const INIT_CHECK_INTERVAL: Duration = Duration::from_millis(50);
		const INIT_MESSAGE: &str = "serving on";

		let start_time = Instant::now();
		let mut initialized = false;

		while start_time.elapsed() < INIT_TIMEOUT {
			if let Ok(logs) = read_to_string(&stderr_log) {
				if logs.contains(INIT_MESSAGE) {
					initialized = true;
					break;
				}
			}
			std::thread::sleep(INIT_CHECK_INTERVAL);
		}

		if !initialized {
			return Err(std::io::Error::new(
				std::io::ErrorKind::TimedOut,
				"Server initialization timed out",
			));
		}

		self.instances.insert(
			name.to_string(),
			ProcessInstance {
				process,
				directory_path,
			},
		);

		Ok(())
	}

	/// Convenience helper to start a server with the default local configuration, optionally connected to a named registry.
	fn start_default_local_instance(
		&mut self,
		name: &str,
		registry_name: Option<&str>,
	) -> std::io::Result<()> {
		let config_json = if let Some(registry_name) = registry_name {
			if let Some(registry_instance) = self.get_instance(registry_name) {
				let registry_path = registry_instance.server_path().join("socket");
				let registry_url = format!(
					"http+unix://{}",
					urlencoding::encode(&registry_path.display().to_string())
				);
				// Set up local server.
				indoc::formatdoc! {r#"
					{{
						"advanced": {{
							"error_trace_options": {{
								"internal": true
							}}
						}},
						"remotes": {{
							"default": {{
								"url": "{registry_url}"
							}}
						}},
						"tracing": {{
							"filter": "tangram_server=trace",
							"format": "json"
						}},
						"vfs": null
					}}"#
				}
			} else {
				return Err(std::io::Error::new(
					std::io::ErrorKind::NotFound,
					"no such registry instance available",
				));
			}
		} else {
			indoc::formatdoc! {r#"
				{{
					"advanced": {{
						"error_trace_options": {{
							"internal": true
						}}
					}},
					"remotes": null,
					"tracing": {{
						"filter": "tangram_server=trace",
						"format": "json"
					}},
					"vfs": null
				}}"#
			}
		};
		// Set up local server.
		self.start_instance(name, &config_json)
	}

	/// Convenience helper to start a server with the default registry configuration.
	fn start_default_registry_instance(&mut self, name: &str) -> std::io::Result<()> {
		let registry_config_json = indoc::formatdoc! {r#"
	{{
		"advanced": {{
			"error_trace_options": {{
				"internal": true
			}}
		}},
		"registry": true,
		"remotes": null,
		"tracing": {{
			"filter": "tangram_server=trace",
			"format": "json"
		}},
		"vfs": null
	}}"#
		};
		self.start_instance(name, &registry_config_json)
	}

	/// Stop an individual server instance.
	fn stop_instance(&mut self, name: &str) -> std::io::Result<()> {
		if let Some(mut instance) = self.instances.remove(name) {
			instance.process.kill()?;
			instance.process.wait()?;
		}
		Ok(())
	}

	fn stop_all(&mut self) -> std::io::Result<()> {
		for (_, mut instance) in self.instances.drain() {
			instance.process.kill()?;
			instance.process.wait()?;
		}
		Ok(())
	}
}

impl Drop for Harness {
	fn drop(&mut self) {
		let _ = self.stop_all();
	}
}

impl ProcessInstance {
	/// Create a [`std::process::Command`] to invoke a specific `tg` instance.
	fn command(&self) -> Command {
		let mut cmd = Command::new(&*TG_EXECUTABLE);
		cmd.arg("--config")
			.arg(self.config_path())
			.arg("--path")
			.arg(self.server_path());
		cmd
	}

	/// Get the stdout log for this process.
	fn stdout(&self) -> std::io::Result<String> {
		self.read_log_file(self.stdout_log_path())
	}

	/// Get the stderr log for this process.
	fn stderr(&self) -> std::io::Result<String> {
		self.read_log_file(self.stderr_log_path())
	}

	fn read_log_file(&self, path: impl AsRef<Path>) -> std::io::Result<String> {
		let file = File::open(path.as_ref())?;
		let mut reader = BufReader::new(file);
		let mut contents = String::new();
		reader.read_to_string(&mut contents)?;
		Ok(contents)
	}

	fn config_path(&self) -> PathBuf {
		self.directory_path.join("config.json")
	}

	fn server_path(&self) -> PathBuf {
		self.directory_path.join(".tangram")
	}

	fn stdout_log_path(&self) -> PathBuf {
		self.directory_path.join("stdout.log")
	}

	fn stderr_log_path(&self) -> PathBuf {
		self.directory_path.join("stderr.log")
	}
}

#[test]
fn test_local_server_health() {
	let harness = Harness::new().unwrap();
	let mut harness = scopeguard::guard(harness, |harness| {
		drop(harness);
	});

	harness.start_default_local_instance("local", None).unwrap();

	// Check that we can query the server health.
	let output = harness
		.tg("local")
		.unwrap()
		.arg("server")
		.arg("health")
		.output()
		.unwrap();

	let (server_stdout, server_stderr) = harness.get_logs("local").unwrap().unwrap();

	harness.stop_all().unwrap();

	let stdout = String::from_utf8(output.stdout).expect("stdout was not utf8");

	assert!(stdout.contains("\"version\": \"0.0.0\""));

	// There should be nothing logged to server stdout.
	assert!(server_stdout.is_empty());

	// Server stderr should have logged a "received request" and "received response".
	assert!(server_stderr.contains("received request"));
	assert!(server_stderr.contains("sending response"));
}

#[test]
fn test_basic_build() {
	let harness = Harness::new().unwrap();
	let mut harness = scopeguard::guard(harness, |harness| {
		drop(harness);
	});

	harness.start_default_local_instance("local", None).unwrap();

	// Get the test package.
	let test_package_path = test_package_path("five").unwrap();

	// Build the package.
	let output = harness
		.tg("local")
		.unwrap()
		.arg("build")
		.arg(test_package_path)
		.arg("--quiet")
		.output()
		.unwrap();

	harness.stop_all().unwrap();

	let expected = "5\n";
	let stdout = String::from_utf8(output.stdout).unwrap();
	assert_eq!(stdout, expected);
}

#[test]
fn test_cache_hit_after_push() {
	let harness = Harness::new().unwrap();
	let mut harness = scopeguard::guard(harness, |harness| {
		drop(harness);
	});

	harness.start_default_registry_instance("registry").unwrap();

	harness
		.start_default_local_instance("initial", Some("registry"))
		.unwrap();

	// Get the test package.
	let test_package_path = test_package_path("five").unwrap();

	let get_build_id = |stdout: String| -> Option<String> {
		stdout
			.split(|c: char| c.is_whitespace() || c == '\u{1b}')
			.find(|word| word.starts_with("bld_"))
			.map(|word| word.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_'))
			.map(String::from)
	};

	// Build the package.
	let initial_build_output = harness
		.tg("initial")
		.unwrap()
		.arg("build")
		.arg(&test_package_path)
		.output()
		.unwrap();
	let initial_build_stdout = String::from_utf8(initial_build_output.stdout).unwrap();
	let initial_build_id = get_build_id(initial_build_stdout).unwrap();

	// Push the build.
	let build_push_status = harness
		.tg("initial")
		.unwrap()
		.arg("push")
		.arg(initial_build_id.clone())
		.status()
		.unwrap();
	assert!(build_push_status.success());

	// Destroy the local server.
	harness.stop_instance("initial").unwrap();

	// Start a new fresh local server.
	harness
		.start_default_local_instance("second", Some("registry"))
		.unwrap();

	// We should ge able to "get" the same build ID, via the registry.
	let get_first_build_id_from_second_server_output = harness
		.tg("second")
		.unwrap()
		.arg("get")
		.arg(&initial_build_id)
		.output()
		.unwrap();
	let get_first_build_id_from_second_server_stdout =
		String::from_utf8(get_first_build_id_from_second_server_output.stdout).unwrap();

	// Stop the registry.
	harness.stop_instance("registry").unwrap();

	// Trye getting the build ID again, this time the output should be empty.
	let get_first_build_id_from_second_server_again_output = harness
		.tg("second")
		.unwrap()
		.arg("get")
		.arg(&initial_build_id)
		.output()
		.unwrap();
	let get_first_build_id_from_second_server_again_stdout =
		String::from_utf8(get_first_build_id_from_second_server_again_output.stdout).unwrap();

	harness.stop_all().unwrap();

	// We should have received a build ID from the initial build.
	assert!(initial_build_id.contains("bld_"));
	// We should have successfully retrieved the same build ID from a fresh server via the registry.
	assert!(get_first_build_id_from_second_server_stdout.contains("succeeded"));
	// With the registry stopped, we should have not been able to look up the ID again.
	assert!(get_first_build_id_from_second_server_again_stdout.is_empty());
}

#[test]
fn test_push_object_to_registry() {
	let harness = Harness::new().unwrap();
	let mut harness = scopeguard::guard(harness, |harness| {
		drop(harness);
	});

	harness.start_default_registry_instance("registry").unwrap();

	harness
		.start_default_local_instance("local", Some("registry"))
		.unwrap();

	// Get the test package.
	let test_package_path = test_package_path("five").unwrap();

	// Checkin the directory.
	let package_checkin_output = harness
		.tg("local")
		.unwrap()
		.arg("checkin")
		.arg(&test_package_path)
		.output()
		.unwrap();
	let package_checkin_stdout = String::from_utf8(package_checkin_output.stdout).unwrap();
	let package_id: String = package_checkin_stdout
		.chars()
		.filter(|c| !c.is_whitespace())
		.collect();

	// Get the ID.
	let local_get_output = harness
		.tg("local")
		.unwrap()
		.arg("get")
		.arg(&package_id)
		.output()
		.unwrap();
	let local_get_stdout = String::from_utf8(local_get_output.stdout).unwrap();

	// Push the directory to the registry.
	let _ = harness
		.tg("local")
		.unwrap()
		.arg("push")
		.arg(&package_id)
		.status()
		.unwrap();

	// Stop the local server.
	harness.stop_instance("local").unwrap();

	// Get the ID from the registry.
	let registry_get_output = harness
		.tg("registry")
		.unwrap()
		.arg("get")
		.arg(&package_id)
		.output()
		.unwrap();
	let registry_get_stdout = String::from_utf8(registry_get_output.stdout).unwrap();

	harness.stop_all().unwrap();

	// The registry should have returned an identical object to the local server.
	assert_eq!(local_get_stdout, registry_get_stdout);
}

#[test]
fn test_push_tag() {
	let harness = Harness::new().unwrap();
	let mut harness = scopeguard::guard(harness, |harness| {
		drop(harness);
	});

	harness.start_default_registry_instance("registry").unwrap();

	harness
		.start_default_local_instance("local", Some("registry"))
		.unwrap();

	// Get the test package.
	let test_package_path = test_package_path("five").unwrap();

	// Tag the test package.
	let tag_package_status = harness
		.tg("local")
		.unwrap()
		.arg("tag")
		.arg("test")
		.arg(&test_package_path)
		.status()
		.unwrap();
	assert!(tag_package_status.success());

	// Verify the tag is listed locally.
	let local_tag_list_output = harness
		.tg("local")
		.unwrap()
		.arg("tag")
		.arg("list")
		.arg("test")
		.output()
		.unwrap();
	let local_tag_list_stdout = String::from_utf8(local_tag_list_output.stdout).unwrap();

	// Get the ID associated with the new tag.
	let tag_item_id_output = harness
		.tg("local")
		.unwrap()
		.arg("tag")
		.arg("get")
		.arg("test")
		.output()
		.unwrap();
	let tag_item_id_stdout = String::from_utf8(tag_item_id_output.stdout).unwrap();
	let tag_item_id: String = tag_item_id_stdout
		.chars()
		.filter(|c| !c.is_whitespace())
		.collect();

	// Store the output from `tg get` on that tag.
	let local_directory_get_output = harness
		.tg("local")
		.unwrap()
		.arg("get")
		.arg(&tag_item_id)
		.output()
		.unwrap();
	let local_directory_get_stdout = String::from_utf8(local_directory_get_output.stdout).unwrap();

	// Push the tag.
	let tag_push_output = harness
		.tg("local")
		.unwrap()
		.arg("push")
		.arg("test")
		.output()
		.unwrap();
	let tag_push_stdout = String::from_utf8(tag_push_output.stdout).unwrap();
	dbg!(&tag_push_stdout);

	// Verify the ID is present on the registry.
	let registry_directory_get_output = harness
		.tg("registry")
		.unwrap()
		.arg("get")
		.arg(&tag_item_id)
		.output()
		.unwrap();
	let registry_directory_get_stdout =
		String::from_utf8(registry_directory_get_output.stdout).unwrap();

	// Verify the tag is listed on the registry.
	let registry_tag_list_output = harness
		.tg("registry")
		.unwrap()
		.arg("tag")
		.arg("list")
		.arg("test")
		.output()
		.unwrap();
	let registry_tag_list_stdout = String::from_utf8(registry_tag_list_output.stdout).unwrap();

	harness.stop_all().unwrap();

	// We should see the tag we created with `tg tag list`.
	let expected_tag_list_stdout = "test\n".to_string();
	assert_eq!(local_tag_list_stdout, expected_tag_list_stdout);

	// That tag should have pointed to a directory ID.
	assert!(tag_item_id.contains("dir_"));

	// The local server and the registry should retrun the same output from `tg get`.
	assert_eq!(local_directory_get_stdout, registry_directory_get_stdout);

	// The registry should also show our tag with `tg tag list`.
	assert_eq!(registry_tag_list_stdout, expected_tag_list_stdout);
}
