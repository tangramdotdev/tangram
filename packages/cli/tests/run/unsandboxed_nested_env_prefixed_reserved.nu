use ../../test.nu *

# Setting a TANGRAM_ENV_ prefixed environment variable on a nested unsandboxed tg.run call fails because the prefix is reserved.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(tg.file()).env({
				TANGRAM_ENV_FOO: 5,
			});
		}
	',
}

let output = tg run $path | complete
failure $output
assert ($output.stderr | str contains "env vars prefixed with TANGRAM_ENV_ are reserved")
