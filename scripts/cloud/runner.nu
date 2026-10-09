# Register once, then run the remote runner in the foreground.
let config_path = '.tangram/cloud/runner.json'
if not ($config_path | path exists) {
	let created = ^.tangram/cloud/tangram --config .tangram/cloud/client.json --mode client --url http://127.0.0.1:8476 runner create | from json
	{
		advanced: { disable_version_check: true },
		remotes: {
			default: { token: $created.token.token, url: 'http://127.0.0.1:8476' },
		},
		runner: {
			id: $created.data.id,
			remote: 'default',
			token: $created.token.token,
		},
		vfs: false,
	} | to json | save $config_path
}

exec .tangram/cloud/tangram --config $config_path --directory .tangram/cloud/runner serve
