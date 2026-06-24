use ../../../test.nu *

# tg.Object.withId throws when the id prefix is not a known object kind.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			try {
				tg.Object.withId("xyz_01");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"invalid object id: xyz_01"'
