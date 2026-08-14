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
