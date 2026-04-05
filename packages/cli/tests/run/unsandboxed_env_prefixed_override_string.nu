use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return JSON.stringify({
				type: typeof tg.process.env.FOO,
				value: tg.process.env.FOO,
				prefixed: tg.process.env.TANGRAM_ENV_FOO ?? null,
			});
		}
	',
}

let output = with-env {
	FOO: "5",
	TANGRAM_ENV_FOO: "5",
} {
	tg run --env-string FOO=hello $path
} | from json | from json

assert ($output.type == "string")
assert ($output.value == "hello")
assert ($output.prefixed == null)
