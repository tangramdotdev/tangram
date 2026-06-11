use ../../test.nu *

# An environment variable set on an unsandboxed process is visible both under its plain name and under its TANGRAM_ENV_ prefixed name.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run`printf "%s\\n%s\\n" "$TEST_ENV_INHERIT" "$TANGRAM_ENV_TEST_ENV_INHERIT"`.env({
				TEST_ENV_INHERIT: 42,
			});
		}
	',
}

let output = tg run $path | lines
assert (($output | length) == 2)
assert ($output.0 == "42")
assert ($output.1 == "42")
