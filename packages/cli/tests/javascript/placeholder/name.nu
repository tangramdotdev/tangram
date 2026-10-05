use ../../lib/test.nu *

# A placeholder exposes the name it was constructed with through its name getter.

let local = server spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.placeholder("foo").name; }'
}

let output = tg build $path
snapshot $output '"foo"'
