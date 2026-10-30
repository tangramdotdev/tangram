use {
	super::{Config, Dependency, Format},
	crate::prelude::*,
	std::collections::BTreeMap,
};

#[test]
fn every_format_parses_to_the_same_config() {
	let json = r#"{ "runtime": "env/^1", "args": [{ "version": "1" }], "dependencies": { "greet": { "specifier": "./greet.tg.ts" } } }"#;
	let toml = "runtime = \"env/^1\"\nargs = [{ version = \"1\" }]\n\n[dependencies.greet]\nspecifier = \"./greet.tg.ts\"\n";
	let yaml = "runtime: env/^1\nargs:\n  - version: \"1\"\ndependencies:\n  greet:\n    specifier: ./greet.tg.ts\n";
	let dependencies = BTreeMap::from([(
		"greet".to_owned(),
		Dependency::Map {
			attributes: BTreeMap::new(),
			specifier: "./greet.tg.ts".to_owned(),
		},
	)]);
	for (text, format) in [
		(json, Format::Json),
		(toml, Format::Toml),
		(yaml, Format::Yaml),
	] {
		let config = Config::parse(text, format).unwrap();
		let args = config
			.args
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		assert_eq!(args, [r#"{"version":"1"}"#], "{format:?}");
		assert_eq!(config.dependencies, dependencies, "{format:?}");
		assert_eq!(config.runtime, Dependency::String("env/^1".to_owned()));
	}
}

#[test]
fn a_config_rejects_unknown_keys_and_values_that_are_not_values() {
	for (text, format) in [
		(r#"{ "runtime": "env", "imports": {} }"#, Format::Json),
		(r#"{ "dependencies": {} }"#, Format::Json),
		(
			r#"{ "runtime": { "specifier": "env", "kind": "x" } }"#,
			Format::Json,
		),
		(
			"runtime = \"env\"\nargs = [1979-05-27T07:32:00Z]\n",
			Format::Toml,
		),
	] {
		let error = Config::parse(text, format).unwrap_err();
		assert_eq!(error.kind(), Some(tg::error::Kind::Argument), "{text}");
	}
}

#[test]
fn formats_follow_the_file_name() {
	for (name, format) in [
		("tangram.json", Some(Format::Json)),
		("env.tg.toml", Some(Format::Toml)),
		("tangram.yaml", Some(Format::Yaml)),
		("env.tg.yml", None),
		("tangram.ts", None),
	] {
		assert_eq!(Format::for_path(name), format, "{name}");
	}
}

#[test]
fn parse_errors_have_ranges_in_every_format() {
	for (text, format, line) in [
		(
			"{\n  \"runtime\": \"env\",\n  \"imports\": {}\n}",
			Format::Json,
			2,
		),
		("runtime = \"env\"\nimports = {}\n", Format::Toml, 1),
		("runtime: env\nimports: {}\n", Format::Yaml, 1),
	] {
		let (_, range) = Config::parse_with_range(text, format).unwrap_err();
		assert_eq!(range.unwrap().start.line, line, "{format:?}");
	}
}
