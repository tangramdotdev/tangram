use ../../../test.nu *

# tg.Value.print wraps a value in ANSI color codes when the color option is set.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.print(42, { color: true });'
}

let output = tg build $path
snapshot $output '"\u001b[93m42\u001b[0m"'
