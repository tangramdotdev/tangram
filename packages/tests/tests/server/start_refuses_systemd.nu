use ../../util/test.nu *

# Where systemd is running, with a config that declares systemd as the manager, neither `--start` nor `tangram server start` starts a server, `tangram server stop` and `tangram server restart` refuse, and each fails with a hint to use systemctl.

if not (systemd_running) {
	skip_test 'this test requires a system on which systemd is running'
}

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{ manager: systemd, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram health | complete
failure $output 'the command should fail without a server'
assert ($output.stderr | str contains 'no server is running') $output.stderr
assert ($output.stderr | str contains '`systemctl start tangram`') $output.stderr

let output = tangram ...$tangram --start health | complete
failure $output '`--start` should not start a server that systemd manages'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr
assert ($output.stderr | str contains '`systemctl start tangram`') $output.stderr

let output = tangram ...$tangram server start | complete
failure $output '`tangram server start` should not start a server that systemd manages'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr
assert ($output.stderr | str contains '`systemctl start tangram`') $output.stderr

let output = tangram ...$tangram server stop | complete
failure $output '`tangram server stop` should refuse for a server that systemd manages'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr
assert ($output.stderr | str contains '`systemctl stop tangram`') $output.stderr

let output = tangram ...$tangram server restart | complete
failure $output '`tangram server restart` should refuse for a server that systemd manages'
assert ($output.stderr | str contains 'the server is managed by systemd') $output.stderr
assert ($output.stderr | str contains '`systemctl restart tangram`') $output.stderr

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { manager: systemd, status: stopped }
