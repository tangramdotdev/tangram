use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "." with { type: "directory" };
	'
}

tg check $path
