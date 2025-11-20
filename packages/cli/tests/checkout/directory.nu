use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"hello.txt": "Hello, World!",
			})
		}
	'
}

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout $id $checkout_path
snapshot -n result --path $checkout_path
