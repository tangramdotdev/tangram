use ../../../test.nu *

# tg.template merges adjacent string arguments into a single component.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return (await tg.template("a", "b", "c")).components; }'
}

let output = tg build $path
snapshot $output '["abc"]'
