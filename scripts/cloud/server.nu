let config = {
	advanced: {
		disable_version_check: true,
		single_directory: false,
		single_process: false,
	},
	cleaner: true,
	database: {
		kind: 'postgres',
		url: 'postgres://postgres@localhost:5432/database',
	},
	http: {
		url: 'http://localhost:8476'
	},
	index: {
		kind: 'postgres',
		url: 'postgres://postgres@localhost:5432/index',
	},
	indexer: {
		message_batch_timeout: 1,
	},
	messenger: {
		kind: 'nats',
		url: 'nats://localhost',
	},
	remotes: [],
	runner: false,
	store: {
		kind: 'scylla',
		addr: 'localhost:9042',
		keyspace: 'store',
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
