use ../../../test.nu *

# A placeholder exposes the name it was constructed with through its name getter.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.placeholder("foo").name;'
}

let output = tg build $path
snapshot $output '"foo"'
