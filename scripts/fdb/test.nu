# Run the FoundationDB index tests against a temporary FoundationDB server.
def main [...args: string] {
	# Create the server directory and the cluster file.
	let directory = mktemp --directory
	let data = $directory | path join data
	let log = $directory | path join log
	mkdir $data $log
	let address = $"127.0.0.1:(port)"
	let cluster = $directory | path join fdb.cluster
	$"test:test@($address)" | save $cluster

	# Start the server.
	let server = job spawn {
		^fdbserver --cluster-file $cluster --datadir $data --logdir $log --listen-address $address --public-address $address | ignore
	}

	# Configure a single process database and wait until it is available.
	^fdbcli --cluster-file $cluster --timeout 30 --exec "configure new single memory" | ignore
	mut available = false
	for _ in 1..60 {
		let status = ^fdbcli --cluster-file $cluster --timeout 5 --exec "status minimal" | complete
		if ($status.stdout | str contains "available") {
			$available = true
			break
		}
		sleep 500ms
	}

	# Run the tests.
	let succeeded = if $available {
		try {
			with-env { FDB_CLUSTER_FILE: $cluster } {
				^cargo test --package tangram_index_fdb -- --include-ignored ...$args
			}
			true
		} catch {
			false
		}
	} else {
		print --stderr "the database did not become available"
		false
	}

	# Stop the server and remove its directory.
	job kill $server
	rm --recursive --force $directory

	if not $succeeded {
		exit 1
	}
}
