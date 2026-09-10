use ../../test.nu *

const archive_path = path self archive.ts

export def spawn_archive [] {
	let port_path = mktemp
	let job = job spawn --description archive {
		bun run $archive_path $port_path
	}
	wait_until {
		open --raw $port_path | str trim | is-not-empty
	} 'the mock archive must start'
	let port = open --raw $port_path | str trim
	{
		config: {
			access_key: 'test',
			# The S3 client prefixes the bucket to the endpoint host.
			bucket: '127',
			endpoint: $'http://0.0.1:($port)',
			express: false,
			kind: 's3',
			pool: { max: 4, min: 0 },
			region: 'test',
			secret_key: 'test',
		},
		job: $job,
		url: $'http://127.0.0.1:($port)',
	}
}
