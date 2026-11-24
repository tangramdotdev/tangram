use super::*;

fn parse<'a>(words: &[&'a str]) -> Result<(Args, Vec<ArgumentKind>), Error<'static, 'a>> {
	let mut argv = words.iter().map(|word| OsStr::new(*word)).collect();
	parse_args(&mut argv)
}

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
		(vec!["--directory", "-"], vec!["-"]),
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
	assert!(matches!(
		parse(&["check", "."]).unwrap().0.command,
		Command::Check(_)
	));
	assert!(matches!(
		parse(&["health"]).unwrap().0.command,
		Command::Health(_)
	));
	assert!(matches!(
		parse(&["r", ".#install"]).unwrap().0.command,
		Command::Run(_)
	));
}

#[test]
fn implicit_run_preserves_help_and_errors() {
	for args in [
		[".#install", "--not-a-real-option"],
		["check", "--not-a-real-option"],
	] {
		assert!(matches!(parse(&args), Err(Error::UnknownFlag { .. })));
	}
	let Err(Error::Help { cmd, .. }) = parse(&[".#install", "--help"]) else {
		panic!("expected run help");
	};
	assert_eq!(cmd.name, "run");
}

#[test]
fn checkout_arguments_require_a_path_when_explicit() {
	assert!(matches!(
		parse(&["run", ".#install", "--checkout="]),
		Err(Error::InvalidValue(_)),
	));
	assert!(parse(&["run", ".#install", "--checkout"]).is_ok());
}

#[test]
fn sandbox_run_preserves_fixed_arity_and_raw_argv() {
	use std::os::unix::ffi::OsStrExt as _;

	let mut argv = [
		"sandbox", "run", "--bind", "/one", "/two", "--bind", "/three", "/four", "--",
	]
	.map(OsStr::new)
	.to_vec();
	let raw = OsStr::from_bytes(b"\xff");
	argv.push(raw);
	let (args, _) = parse_args(&mut argv).unwrap();
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
	assert!(matches!(
		parse(&["run", ".#install", "--view", "inline", "--view", "none"]),
		Err(Error::DuplicateFlag { .. }),
	));
}

#[test]
fn valueless_flags_reject_attached_values() {
	assert!(matches!(
		parse(&["build", ".#install", "--public=false"]),
		Err(Error::UnexpectedArg { .. }),
	));
	assert!(parse(&["run", ".#install", "-a", "--public=false"]).is_ok());
	assert!(parse(&["run", ".#install", "--arg-string="]).is_ok());
}
