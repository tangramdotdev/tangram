use ../../util/test.nu *

# Starting a server that fails before it writes to its log reports the server's output.

let directory = mktemp --directory
let config_path = $directory | path join 'config.json'
{ log: { max_files: 0 }, vfs: false } | to json | save $config_path
let tangram = [--config $config_path --directory ($directory | path join 'server') --no-remotes]

let output = tangram ...$tangram server start | complete
failure $output 'the server should fail to start'
assert ($output.stderr | str contains 'the server exited before it was ready') $output.stderr
assert ($output.stderr | str contains "the log's max_files must be at least 1") $output.stderr
