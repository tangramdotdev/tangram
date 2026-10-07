use super::split_statements;

#[test]
fn split_comments_and_quotes() {
	let source = r#"
		-- A line comment with a semicolon.
		create table "outbox;items" (
			value text default 'a;''b'
		);
		/* A block comment with a semicolon. */
		select * from "outbox;items";
	"#;
	let statements = split_statements(source).unwrap();
	assert_eq!(
		statements,
		[
			r#"create table "outbox;items" (
			value text default 'a;''b'
		)"#,
			r#"select * from "outbox;items""#,
		],
	);
}

#[test]
fn split_rejects_unterminated_input() {
	let error = split_statements("select 'value").unwrap_err();
	assert_eq!(
		error.to_string(),
		"the CQL input ended inside a quote or block comment"
	);
}

#[test]
fn parse_credentials_and_timeout() {
	use clap::Parser as _;
	let args = super::Args::try_parse_from([
		"client",
		"scylla",
		"9042",
		"--username",
		"cassandra",
		"--password",
		"secret",
		"--request-timeout",
		"120",
		"--execute",
		"select release_version from system.local",
	])
	.unwrap();
	assert_eq!(args.username.as_deref(), Some("cassandra"));
	assert_eq!(args.password.as_deref(), Some("secret"));
	assert_eq!(args.request_timeout, 120);
}

#[test]
fn reject_zero_timeout() {
	use clap::Parser as _;
	assert!(
		super::Args::try_parse_from([
			"client",
			"--request-timeout",
			"0",
			"--execute",
			"select release_version from system.local",
		])
		.is_err()
	);
}
