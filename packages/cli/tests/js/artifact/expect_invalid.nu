use ../../../test.nu *

# tg.Artifact.expect throws when the value is an object that is not an artifact.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.blob("hi");
			try {
				tg.Artifact.expect(blob);
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"failed assertion"'
