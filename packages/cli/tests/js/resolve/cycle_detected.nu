use ../../../test.nu *

# tg.resolve throws when the value contains a reference cycle.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let object = {};
			object.self = object;
			try {
				await tg.resolve(object);
				return "did not throw";
			} catch (error) {
				return error.message;
			}
		};
	'
}

let output = tg build $path
snapshot $output '"cycle detected"'
