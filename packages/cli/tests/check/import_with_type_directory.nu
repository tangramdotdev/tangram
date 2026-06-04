use ../../test.nu *

# The check command succeeds for a module that imports the current package with the directory type assertion.

let server = spawn

let path = artifact {
	tangram.ts: '
		import file from "." with { type: "directory" };
	'
}

tg check $path
