use ../../test.nu *

# An environment variable set on a sandboxed process is visible both under its plain name and under its TANGRAM_ENV_ prefixed name.

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";

		export default async function () {
			return await tg.run`printf "%s\\n%s\\n" "$TEST_ENV_INHERIT" "$TANGRAM_ENV_TEST_ENV_INHERIT"`.env(tg.build(busybox)).env({
				TEST_ENV_INHERIT: 42,
			}).sandbox();
		}
	',
}

let output = tg run $path | lines
assert (($output | length) == 2)
assert ($output.0 == "42")
assert ($output.1 == "42")
