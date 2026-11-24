use {super::*, clap::error::ErrorKind};

#[test]
fn implicit_run_matches_explicit_run() {
	for (prefix, trailing) in [
		(vec![], vec![".#install"]),
		(
			vec!["--quiet"],
			vec!["./package#build", "--", "two words", "--help", ""],
		),
		(
			vec!["--directory", ".#install"],
			vec![".#install", "-a", "first", "-A", "true", "-a", "second"],
		),
		(vec![], vec!["some-tag", "--watch", "--", "argument"]),
		(vec![], vec!["--", ".#install"]),
	] {
		let implicit = [prefix.clone(), trailing.clone()].concat();
		let explicit = [prefix, vec!["run"], trailing].concat();
		let (implicit, implicit_order) = parse(&implicit).unwrap();
		let (explicit, explicit_order) = parse(&explicit).unwrap();
		assert!(matches!(implicit.command, Command::Run(_)));
		assert_eq!(format!("{implicit:?}"), format!("{explicit:?}"));
		assert_eq!(implicit_order, explicit_order);
	}
}

#[test]
fn builtins_and_aliases_take_precedence() {
	let (args, _) = parse(&["check", "."]).unwrap();
	assert!(matches!(args.command, Command::Check(_)));
	let (args, _) = parse(&["health"]).unwrap();
	assert!(matches!(args.command, Command::Health(_)));
	let (args, _) = parse(&["r", ".#install"]).unwrap();
	assert!(matches!(args.command, Command::Run(_)));
}

#[test]
fn implicit_run_preserves_help_and_errors() {
	for (args, expected) in [
		(vec![".#install", "--help"], ErrorKind::DisplayHelp),
		(
			vec![".#install", "--not-a-real-option"],
			ErrorKind::UnknownArgument,
		),
		(
			vec!["check", "--not-a-real-option"],
			ErrorKind::UnknownArgument,
		),
	] {
		let error = parse(&args).unwrap_err();
		assert_eq!(error.kind(), expected);
	}
}

#[test]
fn checkout_arguments_require_a_path_when_explicit() {
	assert!(parse(&["run", ".#install", "--checkout="]).is_err());
	assert!(parse(&["run", ".#install", "--checkout"]).is_ok());
}

#[test]
fn sandbox_run_preserves_fixed_arity_and_raw_argv() {
	use std::os::unix::ffi::OsStringExt as _;

	let mut argv = [
		"tg", "sandbox", "run", "--bind", "/one", "/two", "--bind", "/three", "/four", "--",
	]
	.map(OsString::from)
	.to_vec();
	let raw = OsString::from_vec(vec![0xff]);
	argv.push(raw.clone());
	let (args, _) = parse_args(argv).unwrap();
	let Command::Sandbox(tangram_cli_sandbox::Args {
		command: tangram_cli_sandbox::Command::Run(args),
	}) = args.command
	else {
		panic!("expected sandbox run");
	};
	assert_eq!(
		args.options.binds,
		["/one", "/two", "/three", "/four"].map(std::path::PathBuf::from)
	);
	assert_eq!(args.options.command, [raw]);

	assert!(
		parse(&[
			"--config",
			"root.json",
			"sandbox",
			"run",
			"--config-fd",
			"3"
		])
		.is_ok()
	);
	assert!(
		parse(&[
			"sandbox",
			"run",
			"--config",
			"launch.json",
			"--config-fd",
			"3"
		])
		.is_err()
	);
	assert!(parse(&["sandbox", "run", "--bind", "/one", "--", "command"]).is_err());
	assert!(
		parse(&[
			"sandbox",
			"run",
			"--config-fd",
			"3",
			"--bind",
			"/one",
			"/two"
		])
		.is_err()
	);
}

#[test]
fn repeated_scalar_flags_are_rejected() {
	assert_eq!(
		parse(&["run", ".#install", "--view", "inline", "--view", "none"])
			.unwrap_err()
			.kind(),
		ErrorKind::ArgumentConflict
	);
}

#[test]
fn valueless_flags_reject_attached_values() {
	assert!(parse(&["build", ".#install", "--public=false"]).is_err());
	assert!(parse(&["run", ".#install", "-a", "--public=false"]).is_ok());
	assert!(parse(&["run", ".#install", "--arg-string="]).is_ok());
}

fn parse(words: &[&str]) -> clap::error::Result<(Args, Vec<ArgumentKind>)> {
	let argv = std::iter::once("tg")
		.chain(words.iter().copied())
		.map(OsString::from)
		.collect();
	parse_args(argv)
}
