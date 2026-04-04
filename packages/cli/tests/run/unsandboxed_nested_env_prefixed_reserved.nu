use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(() => "ok").env({
				TANGRAM_ENV_FOO: 5,
			});
		}
	',
}

let output = tg run $path | complete
failure $output
assert ($output.stderr | str contains "env vars prefixed with TANGRAM_ENV_ are reserved")
