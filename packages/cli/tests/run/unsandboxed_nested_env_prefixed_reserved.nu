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
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> env vars prefixed with TANGRAM_ENV_ are reserved
	   ╭─[<redacted>/tangram.ts:2:9]
	 1 │ export default async function () {
	 2 │     return await tg.run(tg.file()).env({
	   ·            ▲
	   ·            ╰── env vars prefixed with TANGRAM_ENV_ are reserved
	 3 │         TANGRAM_ENV_FOO: 5,
	   ╰────

'
