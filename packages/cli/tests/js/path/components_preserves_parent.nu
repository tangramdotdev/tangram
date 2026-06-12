use ../../../test.nu *

# tg.path.components preserves parent-directory components rather than resolving them.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.path.components("a/../b");'
}

let output = tg build $path
snapshot $output '["a","..","b"]'
