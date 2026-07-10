use ../../../test.nu *

# An object state without a loaded object represents it with null.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let state = new tg.Object.State({ stored: true });
			return state.object === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
