use ../../../test.nu *

# An explicit null stack is not overwritten by a captured stack, but an omitted stack is captured.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let cleared = tg.error.sync("message", { stack: null });
			let captured = tg.error.sync("message");
			return (await cleared.stack) === null && (await captured.stack) !== null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
