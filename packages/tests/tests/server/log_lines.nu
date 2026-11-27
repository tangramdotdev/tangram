use ../../util/test.nu *

# `tangram server log` prints what the server wrote to its log, and `-n` prints only the last lines.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{
	tracing: { filter: 'tangram_http=trace' },
	vfs: false,
} | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram server start | complete
success $output 'the server should start'
tangram ...$tangram tag get marker-first | complete | ignore
tangram ...$tangram tag get marker-last | complete | ignore

let output = tangram ...$tangram server log | complete
success $output
assert ($output.stdout | str contains '/tags/marker-first') $output.stdout
assert ($output.stdout | str contains '/tags/marker-last') $output.stdout

# The server logs a request and a response for each request, so the last two lines are the last request's.
let output = tangram ...$tangram server log -n 2 | complete
success $output
let lines = $output.stdout | lines
assert equal ($lines | length) 2
assert ($lines.0 | str contains '/tags/marker-last') $output.stdout
assert not ($output.stdout | str contains '/tags/marker-first') $output.stdout

let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'
