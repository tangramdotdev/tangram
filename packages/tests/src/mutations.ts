export default tg.target(() =>
	tg.directory({
		template: template(),
	})
);

export let template = tg.target(async () => {
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
	return (
		await tg.target(
			tg`echo $ENV_VAR > $OUTPUT`,
			{ env: bar },
			{ env: foo },
			{ env: baz }
		)
	).output() as Promise<tg.File>;
});
