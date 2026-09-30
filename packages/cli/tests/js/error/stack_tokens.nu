use ../../lib/test.nu *

# Captured stacks retain their module frames without copying authorization tokens into them.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () {
			for (const error of [new Error("boom"), tg.error.sync("boom")]) {
				const stack = error instanceof Error ? error.stack : error.state.object.value.stack;
				const modules = stack.filter(frame => frame.file.kind === "module");
				tg.assert(modules.length > 0);
				for (const frame of modules) {
					const data = tg.Error.Location.toData(frame);
					tg.assert(Object.values(data.file.value.referent.options?.tokens ?? {}).flat().length === 0);
				}
			}
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
