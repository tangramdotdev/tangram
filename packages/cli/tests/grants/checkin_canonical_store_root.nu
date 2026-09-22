use ../lib/test.nu *
use ../lib/checkin.nu checkin-output

# Canonicalization determines which store root must be authorized.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let secret = tg --token $alice.token put 'tg.directory({ "program": tg.file("secret") })' | str trim
tg --token $alice.token checkout $secret | ignore
let path = $'../($secret)' | to json --raw
let value = ['tg.directory({ "link": tg.symlink({ "path": ' $path ' }) })'] | str join
let source = tg --token $bob.token put $value | str trim
let root = tg --token $bob.token checkout $source | str trim
tg index

# The source root is readable, but its relative symlink does not grant access to the target root.
let path = $root | path join link program
let denied = try {
	checkin-output $server $path --token $bob.token | ignore
	false
} catch {
	true
}
assert $denied 'checkin must authorize the canonical root'

# Once the target is authorized, the raw API path resolves to that root and its subpath.
tg --token $alice.token grant $bob.user.id object_subtree $secret
let output = checkin-output $server $path --token $bob.token
let params = $'http://localhost/($output.reference)' | url parse | get params
assert equal ($params | where key == id | first | get value) $secret
assert equal ($params | where key == path | first | get value) program
assert equal (tg --token $bob.token read $output.reference) secret
