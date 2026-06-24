use ../../../test.nu *

# tg.Template.expect returns the value unchanged when it is a template.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let template = await tg.template("a", "b");
			return tg.Template.expect(template).components;
		}
	'
}

let output = tg build $path
snapshot $output '["ab"]'
