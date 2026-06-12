use ../../../test.nu *

# tg.directory throws when an entry path component is not normal.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			try {
				await tg.directory({ "..": "x" });
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"all path components must be normal"'
