use ../../../test.nu *

# tg.Artifact.withId returns an artifact of the kind named by the id, preserving the id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hi");
			let artifact = tg.Artifact.withId(file.id);
			return artifact instanceof tg.File && artifact.id === file.id;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
