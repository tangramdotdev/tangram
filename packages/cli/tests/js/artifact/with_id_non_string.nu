use ../../../test.nu *

# tg.Artifact.withId throws when the id is not a string.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Artifact.withId(42);
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"expected a string: 42"'
