use ../../../test.nu *

# tg.path.join concatenates a relative path onto an absolute base.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.path.join("/a", "b", "c");'
}

let output = tg build $path
snapshot $output '"/a/b/c"'
