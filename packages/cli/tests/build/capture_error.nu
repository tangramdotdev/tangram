use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => { throw new error("not so fast!"); };'
}

let output = tg build $path | complete
failure $output
