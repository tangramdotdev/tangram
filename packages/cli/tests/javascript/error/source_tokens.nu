use ../../lib/test.nu *

# Error source referents pass their proofs to inline children and stored source handles.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			const error = tg.error.sync("inner");
			const data = tg.Error.toData(error);
			const inline = tg.Error.fromData(tg.Error.Data.withoutLocationAndTokens(data));
			const module = import.meta.module;
			const inlineSource = {
				node: inline.state.object.value,
				options: module.referent.options,
			};
			await error.store();
			const storedSource = {
				node: tg.Error.withId(error.id),
				options: tg.Object.toReferent(error).options,
			};
			for (const source of [inlineSource, storedSource]) {
				const outer = tg.error.sync("outer", { source, stack: null });
				const roundTrip = tg.Error.fromData(JSON.parse(JSON.stringify(tg.Error.toData(outer))));
				const children = tg.Error.Object.children(roundTrip.state.object.value);
				tg.assert(children.length > 0);
				for (const child of children) {
					const tokens = tg.Object.toReferent(child).options.tokens;
					tg.assert(Object.values(tokens).flat().length > 0);
				}
			}
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
