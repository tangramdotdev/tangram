use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(inspect).env({
				TEST_ENV_INHERIT: 42,
			});
		}

		export function inspect() {
			return JSON.stringify({
				type: typeof tg.process.env.TEST_ENV_INHERIT,
				value: tg.process.env.TEST_ENV_INHERIT,
			});
		}
	',
}

let output = tg run $path | from json | from json
assert ($output.type == "number")
assert ($output.value == 42)
