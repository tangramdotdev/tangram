use ../../../test.nu *

# tg.template keeps an artifact as its own component, separating the surrounding string components.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await tg.template("cat ", await tg.file("hi"), " end");'
}

let output = tg build $path | normalize_ids
snapshot $output 'tg.template(["cat ",fil_010000000000000000000000000000000000000000000000000000," end"])'
