use ../../../test.nu *

# tg.Artifact.expect returns the value unchanged when it is an artifact.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let file = await tg.file("hi");
			return tg.Artifact.expect(file) instanceof tg.File;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
