# The FoundationDB server image on macOS, which must match the FoundationDB client that Tangram links.
const foundationdb_image = 'foundationdb/foundationdb:7.3.68'

# Run the FoundationDB index tests against a temporary FoundationDB server.
def main [...args: string] {
	# Check the required programs.
	let programs = if $nu.os-info.name == 'linux' { [fdbcli fdbserver] } else { [docker] }
	let missing = $programs | where { |program| which $program | is-empty }
	if not ($missing | is-empty) {
		error make { msg: $"the following programs are missing from PATH: ($missing | str join ', ')" }
	}

	# Create the server directory and the cluster file.
	let directory = mktemp --directory
	let data = $directory | path join data
	let log = $directory | path join log
	mkdir $data $log
	let port = port
	let address = $"127.0.0.1:($port)"
	let cluster = $directory | path join fdb.cluster
	let container = $"tangram_fdb_test_(random chars --length 8 | str lowercase)"

	# Start the server, natively on Linux and in a Docker container on macOS.
	let server = if $nu.os-info.name == 'linux' {
		$"test:test@($address)" | save $cluster
		job spawn {
			^fdbserver --cluster-file $cluster --datadir $data --logdir $log --listen-address $address --public-address $address | ignore
		}
	} else {
		$"docker:docker@($address)" | save $cluster
		if (^docker image inspect $foundationdb_image | complete).exit_code != 0 {
			print --stderr $"pulling ($foundationdb_image)"
			^docker pull $foundationdb_image
		}
		^docker run --detach --rm --name $container --publish $"($address):($port)" --env FDB_NETWORKING_MODE=host --env $"FDB_PORT=($port)" --tmpfs /var/fdb/data:rw --tmpfs /var/fdb/logs:rw $foundationdb_image | ignore
		null
	}
	let fdbcli = if $nu.os-info.name == 'linux' {
		[fdbcli --cluster-file $cluster]
	} else {
		[docker exec $container fdbcli]
	}

	# Configure a single process database and wait until it is available.
	run-external ...$fdbcli ...[--timeout 30 --exec "configure new single memory"] | complete | ignore
	mut available = false
	for _ in 1..60 {
		let status = run-external ...$fdbcli ...[--timeout 5 --exec "status minimal"] | complete
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
	if $server != null {
		job kill $server
	} else {
		^docker rm --force $container | complete | ignore
	}
	rm --recursive --force $directory

	if not $succeeded {
		exit 1
	}
}
