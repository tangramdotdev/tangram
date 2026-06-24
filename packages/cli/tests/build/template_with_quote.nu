use ../../test.nu *

# A tg template literal containing single quotes around an interpolated artifact placeholder is rendered correctly and matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		import file from "./hello.txt";
		export default function () { return tg`
			other_command

			other_command

			other_command

			echo 'exec ${file} "$@"' >> script.sh
		`; }
	'#,
	hello.txt: 'Hello, World!',
}

let output = tg build $path
snapshot $output
