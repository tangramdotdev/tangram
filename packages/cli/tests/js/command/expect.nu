use ../../../test.nu *

# tg.Command.expect returns the value unchanged when it is a command.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.Command.expect(await tg.command({ host: "builtin", executable: "echo" })) instanceof tg.Command; }'
}

let output = tg build $path
snapshot $output 'true'
