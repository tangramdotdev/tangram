use ../../util/test.nu *

# Force cleaning keeps the `bin` directory of the server's directory, where the standalone installer puts the executables, and removes everything else.

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

# Install an executable in the directory, as the installer does.
let bin = $server_directory | path join 'bin'
mkdir $bin
'#!/bin/sh' | save ($bin | path join 'tool')

let output = tangram ...$tangram clean --force | complete
success $output
assert equal (ls --all $server_directory | get name | path basename) [bin] 'only the bin directory should remain'
assert equal (open ($bin | path join 'tool')) '#!/bin/sh' 'the executable should be unchanged'

let output = tangram ...$tangram server status | complete
success $output
assert equal ($output.stdout | from json) { status: stopped }
