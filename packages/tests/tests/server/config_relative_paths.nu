use ../../util/test.nu *

# The server rejects relative paths in the config that would otherwise depend on its working directory.

let directory = mktemp --directory
let server_directory = $directory | path join 'server'
let config_path = $directory | path join 'config.json'

# Reject a relative TLS certificate path.
{ http: { listeners: [{ url: 'http://localhost:0', tls: { certificate: 'certificate.pem', key: '/key.pem' } }] }, vfs: false } | to json | save -f $config_path
let output = tangram -c $config_path -d $server_directory serve | complete
failure $output 'the server must reject a relative TLS certificate path'
assert ($output.stderr | str contains 'the TLS certificate path must be absolute')

# Reject a relative listener socket path.
{ http: { listeners: [{ url: 'http+unix://relative.sock' }] }, vfs: false } | to json | save -f $config_path
let output = tangram -c $config_path -d $server_directory serve | complete
failure $output 'the server must reject a relative listener socket path'
assert ($output.stderr | str contains 'the HTTP listener socket path must be absolute')

# Reject a relative directory path.
{ directory: 'relative', vfs: false } | to json | save -f $config_path
let output = tangram -c $config_path serve | complete
failure $output 'the server must reject a relative directory path'
assert ($output.stderr | str contains 'the config directory path must be absolute')
