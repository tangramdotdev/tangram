use ../../test.nu *

# A module can import a specific file from a tagged package using the "get" import attribute and read its contents.

let server = spawn
let path = artifact {
	"subdirectory": {
		file.txt: "hello, world!\n"
	}
}
tg tag test $path

let path = artifact {
	tangram.ts: '
		import file from "test" with { 
			"get": "subdirectory/file.txt", 
			"type": "file" 
		};
		export default () => file.text;
	'
};

let output = tg build $path
snapshot $output '"hello, world!"'
