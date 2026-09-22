use ../lib/test.nu *

# Without solving, an unproven dependency must not gain subtree authority through the file token.
let server = server spawn --config { vfs: false }
let dependency = tg put 'tg.file("dependency")' | str trim
let path = artifact (file --xattrs { 'user.tangram.dependencies': ([$dependency] | to json) } 'output')
let id = tg checkin --destructive --no-ignore --no-solve $path | str trim
let checkout = $server.checkout_directory | path join $id
let token = xattr_read user.tangram.token $checkout
let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.resource $id
assert equal $body.permissions [object_node]
server stop $server
