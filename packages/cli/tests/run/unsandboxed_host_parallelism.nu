use ../../test.nu *

# tg.host.parallelism reports a positive value to an unsandboxed process.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg.host.parallelism; }
	',
}

let output = tg run $path | from json
assert ($output > 0)
