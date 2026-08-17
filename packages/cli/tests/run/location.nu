use ../../test.nu *

# A location implies a sandbox and cannot be combined with --no-sandbox.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => 42;'
}

# The local location implies a sandbox.
let output = tg run --detach --local --verbose $path | from json
assert equal $output.location local
assert ($output.process | str starts-with 'pcs_')

# An explicit location cannot be combined with an unsandboxed process.
let output = tg run --local --no-sandbox $path | complete
failure $output
assert ($output.stderr | str contains 'a location is not supported without a sandbox')
