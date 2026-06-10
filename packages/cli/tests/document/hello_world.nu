use ../../test.nu *

# Documenting a package with a default export produces documentation JSON that matches the snapshot.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";
	'
}

# Document.
let output = do { cd $path; tg document } | complete
success $output

let json = $output.stdout | from json
snapshot ($json | to json --indent 2)
