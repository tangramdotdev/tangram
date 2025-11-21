use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let dependency = await tg.directory({
				"file.txt": "contents",
				"link": tg.symlink("file.txt"),
			});
			let id = dependency.id;
			return tg.directory({
				"foo.txt": tg.file("foo", { dependencies: { [id]: { item: dependency }}}),
				"bar.txt": tg.file("bar", { dependencies: { [id]: { item: dependency }}})
			});
		}
	'
}

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $checkout_path
snapshot --path $checkout_path
