use ../../../test.nu *

# tg.path.components splits an absolute path into a root component followed by its normal components.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.path.components("/a/b/c");'
}

let output = tg build $path
snapshot $output '["/","a","b","c"]'
