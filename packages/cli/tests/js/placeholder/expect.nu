use ../../../test.nu *

# tg.Placeholder.expect returns the value unchanged when it is a placeholder.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.Placeholder.expect(tg.placeholder("foo")).name; }'
}

let output = tg build $path
snapshot $output '"foo"'
