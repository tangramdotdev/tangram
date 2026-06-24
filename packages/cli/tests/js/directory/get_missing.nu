use ../../../test.nu *

# A directory's get method throws when the path does not exist.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.directory({ "a": "alpha" });
			try {
				await directory.get("nope");
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"failed to get the directory entry \"nope\""'
