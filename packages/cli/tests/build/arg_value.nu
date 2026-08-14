use ../../test.nu *

# A build forwards a command-line `--arg-value` to the default export function and produces the expected output.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function (arg: { name: string }) { return `Hello, ${arg.name}!`; }'
}

let output = tg build $path --arg-value '{"name":"Tangram"}'
snapshot $output '"Hello, Tangram!"'
