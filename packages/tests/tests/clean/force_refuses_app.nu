use ../../util/test.nu *

# Force cleaning with a config that declares the Tangram app as the manager fails with the refusal to stop the server and removes nothing.

# Remove the inherited URL and token, so that the commands find the server through the config.
hide-env --ignore-errors TANGRAM_URL TANGRAM_TOKEN

let directory = mktemp -d
let server_directory = $directory | path join 'server'
mkdir $server_directory
'data' | save ($server_directory | path join 'data')
let config_path = $directory | path join 'config.json'
{ manager: app, server: { directory: $server_directory }, vfs: false } | to json | save $config_path

let output = tangram --config $config_path --no-remotes clean --force | complete
failure $output 'force cleaning should fail when the server is managed by the app'
assert ($output.stderr | str contains 'the server is managed by the Tangram app') $output.stderr
assert equal (open ($server_directory | path join 'data')) 'data' 'the directory should be kept'
