use ../../../test.nu *

# tg.Value.toData converts each element of an array.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.Value.toData([1, "x", true]);'
}

let output = tg build $path
snapshot $output '[1,"x",true]'
