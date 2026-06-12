use ../../../test.nu *

# tg.Template.join joins string templates with a separator.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => await tg.Template.join(":", "a", "b", "c");'
}

let output = tg build $path
snapshot $output 'tg.template(["a:b:c"])'
