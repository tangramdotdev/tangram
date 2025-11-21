use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg`
			function foo() {
				echo "Hello, World!"

			}
		`;
	'
}

let output = run tg build $path
snapshot $output
