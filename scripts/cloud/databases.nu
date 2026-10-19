# Run the local cloud databases in the foreground with temporary state.
def main [
	--foundationdb-version: string = '7.3.68' # The FoundationDB server version on macOS, which must match the FoundationDB client that Tangram links.
] {
	# Check the required programs.
	let programs = if $nu.os-info.name == 'linux' {
		[bash fdbcli fdbserver initdb nats-server pg_isready postgres scylla]
	} else {
		[bash docker initdb nats-server pg_isready postgres]
	}
	let missing = $programs | where { |program| which $program | is-empty }
	if not ($missing | is-empty) {
		error make { msg: $"the following programs are missing from PATH: ($missing | str join ', ')" }
	}

	# Build the ScyllaDB client for the readiness check.
	cargo build --release --package tangram_scylla_client
	let target = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let scylla = $target | path join release tangram_scylla_client

	# Create the temporary state.
	let state = if $nu.os-info.name == 'linux' {
		mktemp -d --tmpdir-path /var/tmp tangram_cloud_databases_XXXXXX
	} else {
		mktemp -d -t tangram_cloud_databases_XXXXXX
	}
	print -e $"starting the local cloud databases with temporary state in ($state)"

	exec bash -c (supervisor) _ $nu.os-info.name $state $scylla $"foundationdb/foundationdb:($foundationdb_version)"
}

def supervisor [] {
	r#'
set -m
platform=$1
state=$2
scylla_client=$3
foundationdb_image=$4
names=()
pids=()
# Give ScyllaDB enough shards and memory that a std build does not queue its reads until they time out.
scylla_args=(
	--auto-snapshot=false
	--commitlog-segment-size-in-mb=8
	--default-log-level=warn
	--developer-mode=1
	--kernel-page-cache=1
	--max-partition-key-restrictions-per-query=1024
	--memory=4GiB
	--schema-commitlog-segment-size-in-mb=32
	--smp=4
	--unsafe-bypass-fsync=1
	--write-request-timeout-in-ms=20000
)

cleanup() {
	status=$?
	trap - EXIT HUP INT TERM
	if [ "$platform" = macos ]; then
		docker rm --force --volumes tangram_cloud_foundationdb tangram_cloud_scylla >/dev/null 2>&1
	fi
	for pid in "${pids[@]}"; do
		kill -TERM -- "-$pid" 2>/dev/null
	done
	for _ in {1..50}; do
		running=false
		for pid in "${pids[@]}"; do
			kill -0 "$pid" 2>/dev/null && running=true
		done
		$running || break
		sleep 0.1
	done
	for pid in "${pids[@]}"; do
		kill -KILL -- "-$pid" 2>/dev/null
	done
	wait 2>/dev/null
	rm -rf -- "$state"
	exit "$status"
}

check() {
	for index in "${!pids[@]}"; do
		if ! kill -0 "${pids[$index]}" 2>/dev/null; then
			echo "${names[$index]} exited:" >&2
			tail -n 40 "$state/logs/${names[$index]}.log" >&2
			exit 1
		fi
	done
}

# Use the disk-backed storage engine, because a realistic remote build fills the memory engine's 1 GiB limit.
configure_foundationdb() {
	"${fdbcli[@]}" --timeout 5 --exec 'configure new single ssd' ||
		"${fdbcli[@]}" --timeout 5 --exec 'status minimal' | grep -q 'is available'
}

ready() {
	name=$1
	shift
	until "$@" >/dev/null 2>&1; do
		check
		sleep 0.1
	done
	echo "$name is ready" >&2
}

start() {
	names+=("$1")
	"${@:2}" >"$state/logs/$1.log" 2>&1 &
	pids+=("$!")
}

trap cleanup EXIT
trap 'exit 130' HUP INT TERM

# Start the databases, each in its own process group.
mkdir "$state/logs"
initdb --auth=trust --no-instructions --pgdata="$state/postgres" --username=postgres >"$state/logs/initdb.log" 2>&1 || exit 1
start postgres postgres -D "$state/postgres" -h 127.0.0.1 -k '' -p 5432 -c fsync=off -c full_page_writes=off -c max_connections=1024 -c synchronous_commit=off
start nats nats-server --addr=127.0.0.1 --port=4222
if [ "$platform" = linux ]; then
	mkdir "$state/fdb" "$state/scylla"
	echo 'local:local@127.0.0.1:4500' >"$state/fdb.cluster"
	echo 'cluster_name: tangram_cloud' >"$state/scylla.yaml"
	start foundationdb fdbserver --cluster-file="$state/fdb.cluster" --datadir="$state/fdb" --listen-address=127.0.0.1:4500 --logdir="$state/fdb" --public-address=127.0.0.1:4500
	start scylla scylla "${scylla_args[@]}" --api-address=127.0.0.1 --listen-address=127.0.0.1 --options-file="$state/scylla.yaml" --overprovisioned --rpc-address=127.0.0.1 --workdir="$state/scylla"
	fdbcli=(fdbcli -C "$state/fdb.cluster")
else
	docker rm --force --volumes tangram_cloud_foundationdb tangram_cloud_scylla >/dev/null 2>&1
	start foundationdb docker run --rm --name tangram_cloud_foundationdb --publish 127.0.0.1:4500:4500 --env FDB_NETWORKING_MODE=host --env FDB_PORT=4500 "$foundationdb_image"
	# Advertise the published address so that clients connect through it, because connections to the container address stall under sustained load on OrbStack and are not routed at all by Docker Desktop.
	start scylla docker run --rm --name tangram_cloud_scylla --publish 127.0.0.1:9042:9042 scylladb/scylla:2026.3.0 "${scylla_args[@]}" --broadcast-rpc-address=127.0.0.1 --overprovisioned=1
	fdbcli=(docker exec tangram_cloud_foundationdb fdbcli)
fi
set +m

# Wait for the databases to become ready.
ready FoundationDB configure_foundationdb
ready NATS bash -c 'exec 3<>/dev/tcp/127.0.0.1/4222 && IFS= read -r -t 2 line <&3 && [[ $line == INFO* ]]'
ready PostgreSQL pg_isready --host=127.0.0.1 --port=5432 --timeout=5
ready ScyllaDB "$scylla_client" 127.0.0.1 9042 -e 'select release_version from system.local'
echo 'the local cloud databases are ready; press Ctrl-C to stop them' >&2

# Stop the remaining databases when any database exits.
while true; do
	check
	sleep 1
done
'#
}
