use ../../../test.nu *

# tg.path.components drops current-directory components and empty components produced by repeated separators.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.path.components("a/./b//c");'
}

let output = tg build $path
snapshot $output '["a","b","c"]'
