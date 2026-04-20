let cluster = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster
let config = {
	advanced: {
		disable_version_check: true,
		single_directory: false,
		single_process: false,
	},
	cleaner: true,
	database: {
		kind: 'postgres',
		url: 'postgres://root@localhost:26257/database?sslmode=disable',
	},
	http: {
		url: 'http://localhost:8476'
	},
	index: {
		cluster: $cluster,
		kind: 'fdb',
	},
	indexer: true,
	messenger: {
		kind: 'nats',
		url: 'nats://localhost:4222',
	},
	object: {
		store: {
			addr: 'localhost:9042',
			keyspace: 'objects',
			kind: 'scylla',
		},
	},
	process: {
		store: {
			kind: 'postgres',
			url: 'postgres://postgres@localhost:5432/processes',
		},
	},
	remotes: [],
	runner: false,
	telemetry: {
		endpoint: 'http://localhost:4317',
		service_name: 'server',
	},
	vfs: false,
	watchdog: {
		batch_size: 100,
		interval: 1,
		ttl: 60
	}
}
let config_path = mktemp -t
let directory_path = mktemp -d
$config | to json | save -f $config_path
exec cargo run --all-features -- serve --config $config_path --directory $directory_path
