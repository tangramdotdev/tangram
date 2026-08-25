use ../../test.nu *

# A watched checkin fails when the server is not in single-process mode.

let server = server spawn --config { advanced: { single_process: false } }
let path = artifact {
	tangram.ts: 'export default "hello";'
}
let output = tg --url $server.url checkin $path --watch | complete

failure $output
assert ($output.stderr | str contains "the watch option is not supported in multi-process mode")
