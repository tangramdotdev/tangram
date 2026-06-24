use ../../../test.nu *

# tg.Template.join omits empty templates so no separator is inserted for them.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await tg.Template.join(":", "a", "", "c"); }'
}

let output = tg build $path
snapshot $output 'tg.template(["a:c"])'
