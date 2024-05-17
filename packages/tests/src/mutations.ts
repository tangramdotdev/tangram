export default tg.target(() =>
	tg.directory({
		template: template(),
	}),
);

export let template = tg.target(() => {
	let bar = {
		ENV_VAR: tg.mutation({
			kind: "set",
			value: "bar",
		}),
	};
	let foo = {
		ENV_VAR: tg.mutation({
			kind: "prefix",
			template: "foo",
			separator: ":",
		}),
	};
	let baz = {
		ENV_VAR: tg.mutation({
			kind: "suffix",
			template: "baz",
			separator: ":",
		}),
	};
	return tg.build(
		tg`echo $ENV_VAR > $OUTPUT`,
		{ env: bar },
		{ env: foo },
		{ env: baz },
	) as Promise<tg.File>;
});
