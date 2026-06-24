use ../../test.nu *

# A tg template literal preserves blank lines within its body and the resulting template matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return tg`
			function foo() {
				echo "Hello, World!"

			}
		`; }
	'
}

let output = tg build $path
snapshot $output
