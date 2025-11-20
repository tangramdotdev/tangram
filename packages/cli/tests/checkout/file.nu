use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!")
		}
	'
}

let id = run tg build $path
let checkout_path = $tmp | path join "checkout"
run tg checkout $id $checkout_path
snapshot -n result --path $checkout_path
