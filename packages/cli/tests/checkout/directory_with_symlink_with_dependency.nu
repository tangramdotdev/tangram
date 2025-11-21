use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"foo": tg.symlink({artifact: tg.file("bar")})
			})
		}
	'
}

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $checkout_path
snapshot --path $checkout_path
