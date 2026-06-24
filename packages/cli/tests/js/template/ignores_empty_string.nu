use ../../../test.nu *

# tg.template ignores empty string components when building the template.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return (await tg.template("a", "", "b")).components; }'
}

let output = tg build $path
snapshot $output '["ab"]'
