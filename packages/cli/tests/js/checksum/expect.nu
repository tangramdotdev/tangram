use ../../../test.nu *

# tg.Checksum.expect returns the value unchanged when it is a well-formed checksum.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Checksum.expect("sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"); }'
}

let output = tg build $path
snapshot $output '"sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"'
