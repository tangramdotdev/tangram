use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return "ok";
		}
	',
}

let output = tg run --env-string TANGRAM_ENV_FOO=5 $path | complete
failure $output
assert ($output.stderr | str contains "env vars prefixed with TANGRAM_ENV_ are reserved")
