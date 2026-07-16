use ../../../test.nu *

# A template's objects method returns the artifact components and omits string components.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return (await tg.template("x", await tg.file("hi"), "y")).objects(); }'
}

let output = tg build $path
snapshot --normalize-ids $output '[fil_010000000000000000000000000000000000000000000000000000]'
