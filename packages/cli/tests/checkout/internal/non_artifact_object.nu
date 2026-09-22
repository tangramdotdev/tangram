use ../../lib/test.nu *

# Checking out an object that is not an artifact fails.

let server = server spawn

# Build a command object, which is not an artifact.
let path = artifact {
	tangram.ts: 'export const hello = () => "hello"; export default () => tg.command(hello);'
}
let command = tg build $path | from json

let output = tg checkout $command | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> expected an artifact

'
