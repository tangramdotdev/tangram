use ../../util/test.nu *

# Stopping a server that has already stopped succeeds. A clean shutdown truncates the lock file rather than removing it, so the second stop reads an empty lock file.

let directory = mktemp -d
let output = tg -d $directory server stop | complete
success $output 'stopping a server that has never started should succeed'

let config_path = $directory | path join 'config.json'
{ vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]
let output = tangram ...$tangram server start | complete
success $output

let output = tangram ...$tangram server stop | complete
success $output

let output = tangram ...$tangram server stop | complete
success $output 'stopping an already stopped server should succeed'
