use ../../test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

let server_path = mktemp --directory
let remote = spawn --name remote
let server = spawn --directory $server_path --name local --config {
	vfs: true
	remotes: { default: { url: $remote.url } }
}
vfs assert_mounted $server_path

let id = tg checkin (artifact { file.txt: 'contents' }) | str trim
tg push $id
tg clean
let root = vfs root $server_path $id

assert ((ls $root | get name | path basename) == ['file.txt']) 'unexpected directory entries'
assert ((open --raw ($root | path join 'file.txt')) == 'contents') 'unexpected file contents'
