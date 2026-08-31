let cluster = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster
let config = {
	advanced: {
		disable_version_check: true,
		single_directory: false,
		single_process: false,
	},
	database: {
		kind: 'postgres',
		read: {
			url: 'postgres://postgres@localhost:5432/database?sslmode=disable',
		},
		write: {
			url: 'postgres://postgres@localhost:5432/database?sslmode=disable',
		},
	},
	checkouts: false,
	http: {
		url: 'http://localhost:8476'
	},
	index: {
		cluster: $cluster,
		kind: 'fdb',
	},
	messenger: {
		kind: 'nats',
		url: 'nats://localhost:4222',
	},
	store: {
		addr: 'localhost:9042',
		keyspace: 'store',
		kind: 'scylla',
	},
	process: {
		store: {
			kind: 'postgres',
			url: 'postgres://postgres@localhost:5432/processes',
		},
	},
	remotes: [],
	roles: [http indexer scheduler],
	telemetry: {
		endpoint: 'http://localhost:4317',
		service_name: 'server',
	},
	tracing: {
		output: 'otlp',
	},
	vfs: false,
}
let config_path = mktemp -t
let directory_path = mktemp -d
$config | to json | save -f $config_path
exec cargo run --all-features -- serve --config $config_path --directory $directory_path
