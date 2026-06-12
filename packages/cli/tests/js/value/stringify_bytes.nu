use ../../../test.nu *

# tg.Value.stringify renders a byte string with the tg.bytes form.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.stringify(tg.encoding.utf8.encode("hi"));'
}

let output = tg build $path
snapshot $output '"tg.bytes(\"aGk=\")"'
