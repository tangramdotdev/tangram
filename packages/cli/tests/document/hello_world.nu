use ../../test.nu *

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
snapshot ($json | to json -i 2)
