use ../../../test.nu *

# tg.directory throws when an entry value is a bare number.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			try {
				await tg.directory({ "n": 5 });
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		}
	'
}

let output = tg build $path
snapshot $output '"cannot use number as directory entry without kind"'
