use ../../test.nu *

# Test running a specific file within a package using a subpath reference.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "from root";',
	sub.tg.ts: 'export default () => "from sub";',
}

let id = tg checkin $path
tg index

let sandbox_output = tg run ($id + "?path=sub.tg.ts") --sandbox
snapshot $sandbox_output '"from sub"'

let output = tg run ($id + "?path=sub.tg.ts")
assert equal $output $sandbox_output
