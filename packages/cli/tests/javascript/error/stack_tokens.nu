use ../../lib/test.nu *

# Captured stacks preserve their module referents through error serialization.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () {
			const expected = import.meta.module.referent.options.tokens;
			tg.assert(Object.values(expected).flat().length > 0);
			for (const error of [new Error("boom"), tg.error.sync("boom")]) {
				const stack = error instanceof Error ? error.stack : error.state.object.value.stack;
				const modules = stack.filter(frame => frame.file.kind === "module");
				tg.assert(modules.length > 0);
				for (const frame of modules) {
					const data = tg.Error.Location.toData(frame);
					const roundTrip = tg.Error.Location.fromData(JSON.parse(JSON.stringify(data)));
					const tokens = roundTrip.file.value.referent.options.tokens;
					for (const [location, entries] of Object.entries(expected)) {
						for (const token of entries) {
							tg.assert(tokens[location].includes(token));
						}
					}
				}
			}
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
