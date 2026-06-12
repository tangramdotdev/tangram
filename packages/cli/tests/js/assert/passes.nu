use ../../../test.nu *

# tg.assert returns without throwing when the condition is truthy.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			tg.assert(true);
			return "ok";
		};
	'
}

let output = tg build $path
snapshot $output '"ok"'
