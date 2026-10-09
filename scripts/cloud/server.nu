let config = {
	advanced: {
		disable_version_check: true,
		single_directory: false,
		single_process: false,
	},
	database: {
		kind: 'postgres',
		read: {
			url: 'postgres://postgres@127.0.0.1:5432/tangram_cloud?sslmode=disable',
		},
		write: {
			url: 'postgres://postgres@127.0.0.1:5432/tangram_cloud?sslmode=disable',
		},
	},
	checkouts: false,
	http: {
		listeners: [{ url: 'http://127.0.0.1:8476' }],
	},
	index: {
		cluster: ('.tangram/cloud/fdb.cluster' | path expand),
		instance: 'tangram_cloud/',
		kind: 'fdb',
	},
	indexer: { id: (open --raw .tangram/cloud/indexer | str trim) },
	instance: 'tangram_cloud',
	messenger: {
		kind: 'nats',
		url: 'nats://127.0.0.1:4222',
	},
	cache: {
		addr: '127.0.0.1:9042',
		keyspace: 'tangram_cloud',
		kind: 'scylla',
	},
	remotes: {},
	roles: [api indexer scheduler],
	vfs: false,
}
$config | to json | save -f .tangram/cloud/server.json
exec .tangram/cloud/tangram --config .tangram/cloud/server.json --directory .tangram/cloud/server serve
