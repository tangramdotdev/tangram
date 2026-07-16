use ../../../test.nu *

# tg.template keeps an artifact as its own component, separating the surrounding string components.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await tg.template("cat ", await tg.file("hi"), " end"); }'
}

let output = tg build $path
snapshot --normalize-ids $output 'tg.template(["cat ",fil_010000000000000000000000000000000000000000000000000000," end"])'
