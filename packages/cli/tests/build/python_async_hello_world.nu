use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.py: 'async def default(): return "Hello from async!"'
}

let output = tg build $path
snapshot $output
