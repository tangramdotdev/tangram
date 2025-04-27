use super::Compiler;
use indoc::indoc;
use insta::assert_snapshot;

#[test]
fn test_export_default_target() {
	let text = indoc!(
		"
			export default tg.command(() => {});
		"
	);
	let output = Compiler::transpile_module(text.to_owned())
		.unwrap()
		.transpiled_text;
	assert_snapshot!(output, @r#"
	export default tg.command({
		module: import.meta.module,
		name: "default",
		function: ()=>{}
	});
	"#);
}

#[test]
fn test_export_named_target() {
	let text = indoc!(
		"
			export let named = tg.command(() => {});
		"
	);
	let output = Compiler::transpile_module(text.to_owned())
		.unwrap()
		.transpiled_text;
	assert_snapshot!(output, @r#"
	export let named = tg.command({
		module: import.meta.module,
		name: "named",
		function: ()=>{}
	});
	"#);
}

#[test]
fn test_named_target() {
	let text = indoc!(
		r#"
			tg.command("named", () => {});
		"#
	);
	let output = Compiler::transpile_module(text.to_owned())
		.unwrap()
		.transpiled_text;
	assert_snapshot!(output, @r#"
	tg.command({
		module: import.meta.module,
		name: "named",
		function: ()=>{}
	});
	"#);
}
