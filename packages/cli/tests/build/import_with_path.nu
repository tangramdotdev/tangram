use ../../test.nu *

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
			"path": "subdirectory/file.txt", 
			"type": "file" 
		};
		export default () => file.text;
	'
};

let output = tg build $path
snapshot $output '"hello, world!"'
