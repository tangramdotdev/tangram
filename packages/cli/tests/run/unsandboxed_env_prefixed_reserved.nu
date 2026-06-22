use ../../test.nu *

# Passing a TANGRAM_ENV_ prefixed environment variable via --env-string fails because the prefix is reserved.

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
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> failed to spawn the process
	-> env vars prefixed with TANGRAM_ENV_ are reserved
	   key = TANGRAM_ENV_FOO

'
