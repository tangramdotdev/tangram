use ../../../test.nu *

# tg.Object.Id.kind throws when the id prefix is not a known object kind.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			try {
				tg.Object.Id.kind("xyz_01");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"invalid object id: xyz_01"'
