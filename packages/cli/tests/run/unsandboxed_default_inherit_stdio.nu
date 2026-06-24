use ../../test.nu *

# An unsandboxed tg.run command inherits stdio by default so its stdout is captured.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.run("echo hello"); }',
}

let output = tg run $path
snapshot $output 'hello'
