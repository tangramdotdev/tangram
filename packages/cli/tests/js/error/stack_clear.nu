use ../../../test.nu *

# An explicit null stack suppresses capture while an omitted stack captures one.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let cleared = tg.error.sync({ stack: null });
			let captured = tg.error.sync();
			return [
				(await cleared.stack) === null,
				(await captured.stack) !== null,
			];
		}
	'
}

let output = tg build $path
snapshot $output '[true,true]'
