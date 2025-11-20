use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: r#'
		import file from "./hello.txt";
		export default () => tg`
			other_command

			other_command

			other_command

			echo 'exec ${file} "$@"' >> script.sh
		`;
	'#,
	hello.txt: 'Hello, World!',
}

let output = tg build $path | complete
success $output
snapshot $output.stdout
