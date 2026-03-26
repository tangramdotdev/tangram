use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.run("echo hello")
	',
}

let output = tg run $path
snapshot $output 'hello'
