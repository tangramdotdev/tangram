use ../../../test.nu *

# A placeholder returned from a build prints as a tg.placeholder call.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.placeholder("foo");'
}

let output = tg build $path
snapshot $output 'tg.placeholder("foo")'
