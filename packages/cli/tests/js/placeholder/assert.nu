use ../../../test.nu *

# tg.Placeholder.assert narrows the value without throwing when it is a placeholder.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			let placeholder = tg.placeholder("foo");
			tg.Placeholder.assert(placeholder);
			return placeholder.name;
		}
	'
}

let output = tg build $path
snapshot $output '"foo"'
