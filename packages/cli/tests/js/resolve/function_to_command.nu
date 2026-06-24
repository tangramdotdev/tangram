use ../../../test.nu *

# tg.resolve converts a function into a command.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let resolved = await tg.resolve(() => "hi");
			return resolved instanceof tg.Command ? "command" : typeof resolved;
		}
	'
}

let output = tg build $path
snapshot $output '"command"'
