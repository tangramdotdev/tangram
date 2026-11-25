use ../../util/test.nu *

# Force cleaning stops a server that `tangram server start` started and removes the server's entire directory.

# Remove the inherited URL and token, so that the commands find the server through the config.
hide-env --ignore-errors TANGRAM_URL TANGRAM_TOKEN

let directory = mktemp -d
let server_directory = $directory | path join 'server'
let config_path = $directory | path join 'config.json'
{ server: { directory: $server_directory }, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --no-remotes]

let output = tangram ...$tangram server start | complete
success $output 'the server should start'

tangram ...$tangram put --no-tokens 'tg.file("force")' | referent node

let output = tangram ...$tangram clean --force | complete
success $output
assert (not ($server_directory | path exists)) 'the directory should be removed'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
