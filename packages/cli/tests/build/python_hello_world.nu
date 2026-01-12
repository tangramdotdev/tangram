use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.py: 'def default(): return "Hello, World!"'
}

let output = tg build $path
snapshot $output
