use ../../../test.nu *

# tg.path.join discards earlier components when a later argument is absolute.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.path.join("a", "/b");'
}

let output = tg build $path
snapshot $output '"/b"'
