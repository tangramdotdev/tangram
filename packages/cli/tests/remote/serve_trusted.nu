use ../../test.nu *

# The serve command can mark one of its remotes as trusted.

let upstream = server spawn --name upstream
let server = server spawn --name server
server stop $server

let config_path = $server.config_path
let directory = $server.directory
let url = $server.url
let remote = $'default=($upstream.url)'
let job = job spawn -d server {
	tangram -c $config_path -d $directory -u $url --remotes $remote serve --trusted-remotes default
}
wait_until { (tg --url $url health | complete).exit_code == 0 } "the server should start"

let remote = tg --url $url remote get default | from json
assert equal $remote.trusted true "the serve option should mark the remote as trusted"

tg -d $directory server stop
try { job kill $job }
