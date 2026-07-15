use ../../test.nu *

# Setting a TANGRAM_ENV_ prefixed environment variable on a sandboxed process fails because the prefix is reserved.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(tg.file()).env({
				TANGRAM_ENV_FOO: 5,
			}).sandbox();
		}
	',
}

let output = tg run $path | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> the child process failed
	   id = pcs_0011111111111111111111111111
	   ╭─[<redacted>/tangram.ts:2:9]
	 1 │ export default async function () {
	 2 │     return await tg.run(tg.file()).env({
	   ·            ▲
	   ·            ╰── the child process failed
	 3 │         TANGRAM_ENV_FOO: 5,
	   ╰────
	-> failed to run the process
	   process = pcs_0011111111111111111111111111
	-> env vars prefixed with TANGRAM_ENV_ are reserved
	   key = TANGRAM_ENV_FOO

'
