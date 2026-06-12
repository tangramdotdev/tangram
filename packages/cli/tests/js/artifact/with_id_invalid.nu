use ../../../test.nu *

# tg.Artifact.withId throws when the id prefix is not an artifact kind.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Artifact.withId("cmd_01");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"invalid artifact id: cmd_01"'
