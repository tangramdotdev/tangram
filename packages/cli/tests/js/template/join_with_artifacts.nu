use ../../lib/test.nu *

# tg.Template.join inserts the separator between artifact components.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await tg.Template.join(" ", await tg.file("a"), await tg.file("b")); }'
}

let output = tg build $path
snapshot --normalize-ids $output 'tg.template([fil_010000000000000000000000000000000000000000000000000000?tokens[local][0]=<token>&tokens[local][1]=<token>," ",fil_011111111111111111111111111111111111111111111111111111?tokens[local][0]=<token>&tokens[local][1]=<token>])'
