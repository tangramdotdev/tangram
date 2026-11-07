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
		let command = Args::command();
		let implicit = [vec!["tg"], prefix.clone(), trailing.clone()].concat();
		let explicit = [vec!["tg"], prefix, vec!["run"], trailing].concat();
		let (implicit_matches, implicit) =
			parse_args(&command, implicit.into_iter().map(Into::into).collect()).unwrap();
		let (explicit_matches, explicit) =
			parse_args(&command, explicit.into_iter().map(Into::into).collect()).unwrap();
		assert!(matches!(implicit.command, Command::Run(_)));
		assert_eq!(format!("{implicit:?}"), format!("{explicit:?}"));
		let implicit_matches = implicit_matches.subcommand_matches("run").unwrap();
		let explicit_matches = explicit_matches.subcommand_matches("run").unwrap();
		for name in ["spawn.arg_strings", "spawn.arg_values"] {
			assert_eq!(
				implicit_matches
					.indices_of(name)
					.map(Iterator::collect::<Vec<_>>),
				explicit_matches
					.indices_of(name)
					.map(Iterator::collect::<Vec<_>>),
			);
		}
	}
}

#[test]
fn builtins_and_aliases_take_precedence() {
	for (args, expected) in [
		(vec!["tg", "check", "."], "check"),
		(vec!["tg", "health"], "health"),
		(vec!["tg", "r", ".#install"], "run"),
	] {
		let command = Args::command();
		let (matches, args) =
			parse_args(&command, args.into_iter().map(Into::into).collect()).unwrap();
		assert!(!matches!(args.command, Command::Run_(_)));
		assert_eq!(matches.subcommand_name(), Some(expected));
	}
}

#[test]
fn implicit_run_preserves_help_and_errors() {
	for (args, expected) in [
		(vec!["tg", ".#install", "--help"], ErrorKind::DisplayHelp),
		(
			vec!["tg", ".#install", "--not-a-real-option"],
			ErrorKind::UnknownArgument,
		),
		(
			vec!["tg", "check", "--not-a-real-option"],
			ErrorKind::UnknownArgument,
		),
	] {
		let command = Args::command();
		let error = parse_args(&command, args.into_iter().map(Into::into).collect()).unwrap_err();
		assert_eq!(error.kind(), expected);
	}
}
