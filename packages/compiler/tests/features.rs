use {
	tangram_client::prelude::*,
	tangram_compiler::{CheckBackend, Compiler, Options},
};

#[test]
fn javascript_frontend() {
	for kind in [tg::module::Kind::JavaScript, tg::module::Kind::TypeScript] {
		let module = module(kind);
		let text = "import { value } from './value.tg.ts'; export default value;";
		let analysis = Compiler::analyze(&module, text);
		let formatted = Compiler::format(text, kind);
		if cfg!(feature = "javascript") {
			let analysis = analysis.unwrap();
			assert!(analysis.diagnostics.is_empty());
			assert_eq!(analysis.imports.len(), 1);
			let formatted = formatted.unwrap();
			assert!(
				Compiler::analyze(&module, &formatted)
					.unwrap()
					.diagnostics
					.is_empty()
			);
		} else {
			assert!(
				analysis
					.unwrap_err()
					.to_string()
					.contains("the javascript feature is not enabled")
			);
			assert!(
				formatted
					.unwrap_err()
					.to_string()
					.contains("the javascript feature is not enabled")
			);
		}
	}
}

#[test]
fn python_frontend() {
	let module = module(tg::module::Kind::Python);
	let text = "def default(): return 42\n";
	let analysis = Compiler::analyze(&module, text);
	let formatted = Compiler::format(text, module.kind);
	if cfg!(feature = "python") {
		assert!(analysis.unwrap().diagnostics.is_empty());
		let formatted = formatted.unwrap();
		assert!(
			Compiler::analyze(&module, &formatted)
				.unwrap()
				.diagnostics
				.is_empty()
		);
		assert_eq!(
			tangram_compiler::load::module(&module, text, None).unwrap(),
			text
		);
	} else {
		assert!(
			analysis
				.unwrap_err()
				.to_string()
				.contains("the python feature is not enabled")
		);
		assert!(
			formatted
				.unwrap_err()
				.to_string()
				.contains("the python feature is not enabled")
		);
	}
}

#[tokio::test]
async fn unavailable_checkers() {
	for (backend, enabled, feature) in [
		(CheckBackend::Tsrs, cfg!(feature = "tsrs"), "tsrs"),
		(
			CheckBackend::Typescript6,
			cfg!(feature = "typescript6"),
			"typescript6",
		),
		(
			CheckBackend::Typescript7,
			cfg!(feature = "typescript7"),
			"typescript7",
		),
	] {
		if enabled {
			continue;
		}
		let options = Options {
			check_backend: backend,
			..Default::default()
		};
		let compiler = compiler(options);
		let error = compiler
			.check(vec![module(tg::module::Kind::TypeScript)])
			.await
			.unwrap_err();
		assert!(
			error
				.to_string()
				.contains(&format!("the {feature} feature is not enabled"))
		);
		compiler.stop();
		compiler.wait().await.unwrap();
	}
	if !cfg!(feature = "ty") {
		let compiler = compiler(Options::default());
		let error = compiler
			.check(vec![module(tg::module::Kind::Python)])
			.await
			.unwrap_err();
		assert!(error.to_string().contains("the ty feature is not enabled"));
		compiler.stop();
		compiler.wait().await.unwrap();
	}
}

fn compiler(options: Options) -> tangram_compiler::Shared {
	let arg = tg::Arg {
		url: Some("http://127.0.0.1:1".parse().unwrap()),
		..Default::default()
	};
	let client = tg::Client::new(arg).unwrap();
	let instance = tg::instance::dynamic::Instance::new(client);
	Compiler::start(
		instance,
		"/test/store".into(),
		"/test/library".into(),
		tokio::runtime::Handle::current(),
		"test".into(),
		options,
	)
}

fn module(kind: tg::module::Kind) -> tg::module::Data {
	let path = match kind {
		tg::module::Kind::JavaScript => "/test/tangram.js",
		tg::module::Kind::Python => "/test/tangram.py",
		tg::module::Kind::TypeScript => "/test/tangram.ts",
		_ => unreachable!(),
	};
	tg::module::Data {
		kind,
		referent: tg::Referent::with_node(tg::module::data::Source::Path(path.into())),
	}
}
