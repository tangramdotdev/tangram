use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.host.parallelism;
	',
}

let output = tg run $path | from json
assert ($output > 0)
