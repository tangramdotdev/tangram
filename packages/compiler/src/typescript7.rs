use {
	crate::{Compiler, check},
	data_encoding::BASE64URL_NOPAD,
	serde_json::{Value, json},
	std::{
		collections::BTreeMap,
		io,
		path::{Component, Path, PathBuf},
		sync::Mutex,
	},
	tangram_client::prelude::*,
	tangram_typescript_client::{Client, protocol},
};

pub struct Service {
	client: tokio::sync::Mutex<Option<Client>>,
	executable: PathBuf,
}

struct Host<'a> {
	compiler: &'a Compiler,
	files: Mutex<Files>,
}

#[derive(Default)]
struct Files {
	texts: BTreeMap<String, String>,
	tokens: BTreeMap<String, tg::authorization::Tokens>,
}

impl Service {
	#[must_use]
	pub fn new(executable: PathBuf) -> Self {
		Self {
			client: tokio::sync::Mutex::new(None),
			executable,
		}
	}

	pub async fn check(
		&self,
		compiler: &Compiler,
		request: check::Request,
	) -> tg::Result<check::Response> {
		let mut guard = self.client.lock().await;
		let host = Host {
			compiler,
			files: Mutex::new(Files::default()),
		};
		let mut client = if let Some(client) = guard.take() {
			client
		} else {
			let mut client = Client::new(&self.executable).await.map_err(|error| {
				tg::error!(
					!error,
					executable = %self.executable.display(),
					"failed to start the typescript 7 service"
				)
			})?;
			client
				.initialize(&host)
				.await
				.map_err(|error| tg::error!(!error, "failed to initialize the typescript 7 API"))?;
			client
		};

		let mut root_files = request
			.modules
			.into_iter()
			.map(|module| host.path(&module))
			.collect::<Vec<_>>();
		root_files.push("/__library__/tangram.d.ts".into());
		let compiler_options = json!({
			"allowJs": true,
			"checkJs": true,
			"exactOptionalPropertyTypes": true,
			"isolatedModules": true,
			"lib": [],
			"module": 99,
			"moduleDetection": 3,
			"noEmit": true,
			"noUncheckedIndexedAccess": true,
			"noUncheckedSideEffectImports": true,
			"skipLibCheck": true,
			"strict": true,
			"target": 99,
			"types": [],
			"verbatimModuleSyntax": true,
		});
		let resolver = client
			.create_module_resolver(&compiler_options, "resolveModuleName/tangram", &host)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the typescript 7 resolver"))?;
		let options = protocol::CreateProgramOptions {
			module_resolver: resolver,
		};
		let program = protocol::CreateProgramParams {
			compiler_options,
			options,
			root_files,
		};
		let params = protocol::CreateSnapshotParams {
			create_programs: vec![program],
			ensure_programs: true,
		};
		let snapshot = client
			.create_snapshot(&params, &host)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the typescript 7 snapshot"))?;
		let project =
			snapshot.operation.created_programs.first().ok_or_else(|| {
				tg::error!("the typescript 7 API did not return a created program")
			})?;

		let diagnostics = client
			.diagnostics(snapshot.snapshot, project, &host)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the typescript 7 diagnostics"))?;
		let diagnostics = diagnostics
			.into_iter()
			.map(|diagnostic| host.diagnostic(&diagnostic))
			.collect::<tg::Result<Vec<_>>>()?;
		client
			.release_snapshot(snapshot.snapshot, &host)
			.await
			.map_err(|error| tg::error!(!error, "failed to release the typescript 7 snapshot"))?;
		client
			.release_module_resolver(resolver, &host)
			.await
			.map_err(|error| tg::error!(!error, "failed to release the typescript 7 resolver"))?;
		guard.replace(client);

		let response = check::Response { diagnostics };

		Ok(response)
	}

	pub async fn stop(&self) {
		if let Some(client) = self.client.lock().await.take()
			&& let Err(error) = client.stop().await
		{
			tracing::warn!(%error, "failed to stop the typescript 7 service");
		}
	}
}

impl Host<'_> {
	fn path(&self, module: &tg::module::Data) -> String {
		self.files.lock().unwrap().path(module)
	}

	async fn callback(&self, method: &str, params: Value) -> io::Result<Value> {
		if method == "resolveModuleName/tangram" {
			let params = serde_json::from_value(params)?;
			return self.resolve(params).await.map_err(io::Error::other);
		}
		let path = params
			.as_str()
			.ok_or_else(|| io::Error::other("expected a filesystem path"))?;
		let value = match method {
			"directoryExists" => json!(
				path == "/" || path.starts_with("/__library__") || path.starts_with("/__runtime__")
			),
			"fileExists" => json!(
				try_library_file(path).is_some()
					|| (path.starts_with("/__runtime__/")
						&& try_module_from_path(path)
							.map_err(io::Error::other)?
							.is_some())
			),
			"getAccessibleEntries" => json!({ "directories": [], "files": [] }),
			"readFile" => {
				let text = self.try_read(path).await.map_err(io::Error::other)?;
				let output = text.map_or_else(
					|| json!({ "kind": "missing" }),
					|text| json!({ "kind": "value", "value": text }),
				);
				return Ok(output);
			},
			_ => {
				return Err(io::Error::other(format!(
					"unsupported typescript callback {method}"
				)));
			},
		};
		let output = json!({ "kind": "value", "value": value });

		Ok(output)
	}

	async fn resolve(&self, params: protocol::ResolveModuleNameParams) -> tg::Result<Value> {
		if params.containing_directory.starts_with("/__library__") {
			let path = Path::new(&params.containing_directory).join(&params.module_name);
			let mut normalized = PathBuf::new();
			for component in path.components() {
				match component {
					Component::CurDir => {},
					Component::ParentDir => {
						normalized.pop();
					},
					component => normalized.push(component),
				}
			}
			let mut path = normalized.to_string_lossy().into_owned();
			if !path.ends_with(".d.ts") {
				if Path::new(&path)
					.extension()
					.is_some_and(|extension| extension == "ts" || extension == "js")
				{
					path.truncate(path.len() - 3);
				}
				path.push_str(".d.ts");
			}
			let output = if try_library_file(&path).is_some() {
				json!({ "resolvedFileName": path })
			} else {
				json!({})
			};
			return Ok(output);
		}

		let path = format!(
			"{}/index.ts",
			params.containing_directory.trim_end_matches('/')
		);
		let referrer = self.files.lock().unwrap().try_module(&path)?;
		let Some(referrer) = referrer else {
			return Ok(json!({}));
		};
		let import = tg::module::Import::with_specifier_and_attributes(&params.module_name, None)?;
		let arg = tg::module::resolve::Arg {
			import,
			referrer: Some(referrer),
		};
		let output = match self.compiler.instance.resolve_module(arg).await {
			Ok(output) => output,
			Err(error) => {
				tracing::debug!(error = %error, "typescript 7 module resolution failed");
				return Ok(json!({}));
			},
		};
		let path = self.path(&output.module);
		let output = json!({ "resolvedFileName": path });

		Ok(output)
	}

	async fn try_read(&self, path: &str) -> tg::Result<Option<String>> {
		if let Some(text) = self.files.lock().unwrap().texts.get(path).cloned() {
			return Ok(Some(text));
		}

		let text = if let Some(file) = try_library_file(path) {
			Some(
				file.contents_utf8()
					.ok_or_else(|| tg::error!("invalid encoding for a library source"))?
					.to_owned(),
			)
		} else if path.starts_with("/__runtime__/") {
			let module = self.files.lock().unwrap().try_module(path)?;
			match module {
				Some(module) => Some(self.compiler.load_module(&module).await?),
				None => None,
			}
		} else {
			None
		};

		if let Some(text) = &text {
			self.files
				.lock()
				.unwrap()
				.texts
				.insert(path.to_owned(), text.clone());
		}

		Ok(text)
	}

	fn diagnostic(&self, diagnostic: &protocol::Diagnostic) -> tg::Result<tg::diagnostic::Data> {
		let location = match (
			&diagnostic.file_name,
			diagnostic.start_position,
			diagnostic.end_position,
		) {
			(Some(path), Some(start), Some(end)) => {
				let files = self.files.lock().unwrap();
				let module = files.try_module(path)?.ok_or_else(
					|| tg::error!(%path, "unknown module in a typescript 7 diagnostic"),
				)?;
				let text = files.texts.get(path).ok_or_else(
					|| tg::error!(%path, "missing source for a typescript 7 diagnostic"),
				)?;

				let end = diagnostic_position(text, end)?;
				let start = diagnostic_position(text, start)?;
				let range = tg::Range { end, start };
				let module = module.without_token();
				let location = tg::module::data::Location { module, range };
				Some(location)
			},
			_ => None,
		};

		let severity = match diagnostic.category {
			0 => tg::diagnostic::Severity::Warning,
			1 => tg::diagnostic::Severity::Error,
			2 => tg::diagnostic::Severity::Hint,
			3 => tg::diagnostic::Severity::Info,
			_ => return Err(tg::error!("unknown category in a typescript 7 diagnostic")),
		};
		let message = diagnostic_message(diagnostic);
		let diagnostic = tg::diagnostic::Data {
			location,
			message,
			severity,
		};

		Ok(diagnostic)
	}
}

impl Files {
	fn path(&mut self, module: &tg::module::Data) -> String {
		let path = module_path(module);
		if !module.referent.options.tokens.is_empty() {
			self.tokens
				.entry(path.clone())
				.or_default()
				.inherit(&module.referent.options.tokens);
		}

		path
	}

	fn try_module(&self, path: &str) -> tg::Result<Option<tg::module::Data>> {
		let Some(mut module) = try_module_from_path(path)? else {
			return Ok(None);
		};
		let path = module_path(&module);
		if let Some(tokens) = self.tokens.get(&path) {
			module.referent.options.tokens = tokens.clone();
		}

		Ok(Some(module))
	}
}

impl tangram_typescript_client::Host for Host<'_> {
	async fn callback(&self, method: &str, params: Value) -> io::Result<Value> {
		self.callback(method, params).await
	}
}

#[must_use]
fn module_path(module: &tg::module::Data) -> String {
	if let (tg::module::Kind::Dts, tg::module::data::Source::Path(path)) =
		(&module.kind, &module.referent.node)
	{
		return format!(
			"/__library__/{}",
			path.to_string_lossy().trim_start_matches("./")
		);
	}
	let identity = module.without_token().to_string();
	let identity = BASE64URL_NOPAD.encode(identity.as_bytes());
	let extension = if module.kind == tg::module::Kind::Js {
		"js"
	} else {
		"ts"
	};
	let path = format!("/__runtime__/{identity}/index.{extension}");

	path
}

fn try_module_from_path(path: &str) -> tg::Result<Option<tg::module::Data>> {
	if let Some(path) = path.strip_prefix("/__library__/") {
		let referent = tg::Referent::with_node(tg::module::data::Source::Path(path.into()));
		let module = tg::module::Data {
			kind: tg::module::Kind::Dts,
			referent,
		};
		return Ok(Some(module));
	}
	let Some(path) = path.strip_prefix("/__runtime__/") else {
		return Ok(None);
	};
	let Some((identity, filename)) = path.split_once('/') else {
		return Ok(None);
	};
	if !matches!(filename, "index.js" | "index.ts") {
		return Ok(None);
	}
	let identity = BASE64URL_NOPAD
		.decode(identity.as_bytes())
		.map_err(|error| tg::error!(!error, "invalid encoding for a typescript 7 module path"))?;
	let identity = std::str::from_utf8(&identity)
		.map_err(|error| tg::error!(!error, "invalid identity in a typescript 7 module path"))?;
	let module = identity
		.parse::<tg::module::Data>()
		.map_err(|error| tg::error!(!error, "failed to decode the typescript 7 module identity"))?;
	let module = module.without_token();

	Ok(Some(module))
}

fn try_library_file(path: &str) -> Option<&'static include_dir::File<'static>> {
	let path = Path::new(path);
	let relative = path.strip_prefix("/__library__").ok();
	let name = path.file_name()?.to_str()?;
	let path = relative.or_else(|| name.starts_with("lib.").then(|| Path::new(name)))?;
	crate::LIBRARY.get_file(path)
}

fn diagnostic_message(diagnostic: &protocol::Diagnostic) -> String {
	let mut message = diagnostic.text.clone();
	for child in &diagnostic.message_chain {
		message.push('\n');
		message.push_str(&diagnostic_message(child));
	}

	message
}

fn diagnostic_position(text: &str, position: protocol::Position) -> tg::Result<tg::Position> {
	let position = tg::Position {
		character: position.character,
		line: position.line,
	};
	let index = position
		.try_to_byte_index_in_string(text, tg::position::Encoding::Utf16)
		.ok_or_else(|| tg::error!("invalid position in a typescript 7 diagnostic"))?;
	let position =
		tg::Position::try_from_byte_index_in_string(text, index, tg::position::Encoding::Utf8)
			.ok_or_else(|| tg::error!("invalid offset in a typescript 7 diagnostic"))?;
	Ok(position)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn module_tokens_do_not_change_identity() {
		let key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let id = tg::file::Id::new(b"module");
		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			)],
			resource: id.clone().into(),
		};
		let token = tg::authorization::Token::sign(body, &key).unwrap();
		let referent = tg::Referent::with_node(tg::module::data::Source::Path("module.ts".into()));
		let module = tg::module::Data {
			kind: tg::module::Kind::Ts,
			referent,
		};
		let mut authorized = module.clone();
		authorized.referent.options.tokens = tg::authorization::Tokens::with_authorization([token]);
		let mut files = Files::default();
		let path = files.path(&module);
		assert_eq!(files.path(&authorized), path);
		assert_eq!(files.path(&module), path);
		assert_eq!(files.tokens.len(), 1);
		assert!(files.texts.is_empty());
		assert_eq!(files.try_module(&path).unwrap().unwrap(), authorized);
		let body = tg::authorization::Body {
			expires_at: i64::MAX - 1,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			)],
			resource: id.into(),
		};
		let token = tg::authorization::Token::sign(body, &key).unwrap();
		let mut additional = module.clone();
		additional.referent.options.tokens = tg::authorization::Tokens::with_authorization([token]);
		assert_eq!(files.path(&additional), path);
		authorized
			.referent
			.options
			.tokens
			.inherit(&additional.referent.options.tokens);
		assert_eq!(files.try_module(&path).unwrap().unwrap(), authorized);
		assert!(
			try_module_from_path(&path)
				.unwrap()
				.unwrap()
				.referent
				.options
				.tokens
				.is_empty()
		);
		let files = Files::default();
		assert_eq!(files.try_module(&path).unwrap().unwrap(), module);
	}

	#[test]
	fn paths_are_independent_of_registration_order() {
		let modules = ["folder with spaces/λ?#.ts", "other.ts"].map(|path| {
			let referent = tg::Referent::with_node(tg::module::data::Source::Path(path.into()));
			tg::module::Data {
				kind: tg::module::Kind::Ts,
				referent,
			}
		});
		let mut forward = Files::default();
		let first = forward.path(&modules[0]);
		let second = forward.path(&modules[1]);
		let mut reverse = Files::default();
		assert_eq!(reverse.path(&modules[1]), second);
		assert_eq!(reverse.path(&modules[0]), first);
		assert_ne!(first, second);
		assert_eq!(
			Files::default().try_module(&first).unwrap().unwrap(),
			modules[0]
		);
		assert!(forward.tokens.is_empty());
		assert!(reverse.tokens.is_empty());
		let directory = Path::new(&first).parent().unwrap();
		let referrer = directory.join("index.ts");
		assert_eq!(
			try_module_from_path(referrer.to_str().unwrap())
				.unwrap()
				.unwrap(),
			modules[0]
		);
	}

	#[test]
	fn javascript_and_declaration_paths_roundtrip() {
		for (kind, path) in [
			(tg::module::Kind::Js, "script.js"),
			(tg::module::Kind::Dts, "tangram/index.d.ts"),
		] {
			let referent = tg::Referent::with_node(tg::module::data::Source::Path(path.into()));
			let module = tg::module::Data { kind, referent };
			let path = module_path(&module);
			assert_eq!(try_module_from_path(&path).unwrap().unwrap(), module);
		}
	}

	#[test]
	fn unicode_diagnostic_positions() {
		let text = "first\nλ😀 value";
		let position = protocol::Position {
			character: 4,
			line: 1,
		};
		let position = diagnostic_position(text, position).unwrap();
		assert_eq!(
			position,
			tg::Position {
				character: 7,
				line: 1
			}
		);
		let position = protocol::Position {
			character: 99,
			line: 1,
		};
		assert!(diagnostic_position(text, position).is_err());
	}
}
