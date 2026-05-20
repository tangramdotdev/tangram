use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A symlink with an artifact resolves through the mounted VFS to the referenced artifact.

let server_path = mktemp --directory
let server = spawn --directory $server_path --config { vfs: true }
vfs assert_mounted $server_path

let id = tg build (artifact {
	tangram.ts: '
		export default async () => {
			let dir = await tg.directory({ "file.txt": tg.file("contents") });
			return tg.directory({
				"dir": dir,
				"link": tg.symlink({ artifact: dir, path: "file.txt" }),
			});
		}
	'
})

let path = vfs root $server_path $id
let target = (^readlink ($path | path join 'link') | str trim)
assert ($target | str contains 'dir_') 'expected the target to reference the artifact by id'
assert ($target | str ends-with 'file.txt') 'unexpected link target'
assert ((open --raw ($path | path join 'link')) == 'contents') 'unexpected contents through the link'
