use ../../../test.nu *

# An explicit null stack suppresses capture while an omitted stack captures one.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let cleared = tg.error({ stack: null });
			let captured = tg.error();
			return [
				(await cleared.stack) === null,
				(await captured.stack) !== null,
			];
		}
	'
}

let output = tg build $path
snapshot $output '[true,true]'
