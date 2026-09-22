use {
	rquickjs as qjs, tangram_client as tg,
	tangram_quickjs::{Deserialize as _, Serde},
};

#[test]
fn concatenated_string_in_mutation() {
	let runtime = qjs::Runtime::new().unwrap();
	let context = qjs::Context::full(&runtime).unwrap();
	context.with(|ctx| {
		let value = ctx.eval::<qjs::Value, _>(r#"({
			kind: "mutation",
			value: {
				kind: "prefix",
				template: { components: [{ kind: "string", value: "first".repeat(1000) + "second".repeat(1000) }] },
				separator: ":"
			}
		})"#).unwrap();
		let Serde(value) = Serde::<tg::value::Data>::deserialize(&ctx, value).unwrap();
		let tg::value::Data::Mutation(tg::mutation::Data::Prefix { separator, template }) = value else {
			panic!("expected a prefix mutation");
		};
		assert_eq!(separator.as_deref(), Some(":"));
		let expected = "first".repeat(1000) + &"second".repeat(1000);
		assert_eq!(template.components, vec![tg::template::data::Component::String(expected)]);
	});
}

#[test]
fn concatenated_string_value() {
	let runtime = qjs::Runtime::new().unwrap();
	let context = qjs::Context::full(&runtime).unwrap();
	context.with(|ctx| {
		let value = ctx
			.eval::<qjs::Value, _>(r#""first".repeat(1000) + "second".repeat(1000)"#)
			.unwrap();
		let Serde(value) = Serde::<tg::value::Data>::deserialize(&ctx, value).unwrap();
		let expected = "first".repeat(1000) + &"second".repeat(1000);
		assert_eq!(value, tg::value::Data::String(expected));
	});
}
