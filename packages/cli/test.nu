#!/usr/bin/env nu

use std/util 'path add'
use ../../scripts/macos/identifiers.nu

use tests/lib/test.nu [database_pool_acquire foundationdb_cluster_description]

const repository_path = path self '../../'
const database_pool_directory_name = 'tangram_test_database_pool'
const foundationdb_container_name = 'tangram_test_foundationdb'
const foundationdb_image = 'foundationdb/foundationdb:7.3.68'
const scylla_container_name = 'tangram_test_scylla'
const scylla_image = 'scylladb/scylla:2026.3.0'

# Build Tangram and the test runner, then run the CLI tests with the runner in packages/cli/test.
def main [
	--accept (-a) # Accept all new and updated snapshots.
	--clean # Clean up leftover test resources from FoundationDB, PostgreSQL, and ScyllaDB.
	--databases # Run the shared cloud databases in the foreground. This is supported on Linux and macOS.
	--jobs (-j): int # The number of concurrent tests to run.
	--kernel-path: path # The path to the linux kernel image to use with --vm. Required when --vm is set.
	--no-cloud # Use local backends for test instances.
	--preserve-failing-temps # Keep the temporary directories for failed tests.
	--preserve-temps # Keep the temporary directories.
	--no-capture # Do not capture the output of each test. This sets --jobs to 1.
	--no-progress-details # Show only the aggregate progress bar, without listing running tests.
	--offline # Skip tests which require network access.
	--print-passing-test-output # Print the output of passing tests.
	--quickjs # Use QuickJS as the JavaScript engine.
	--release # Use a release build of tangram. Some bugs are only observable in release mode.
	--review (-r) # Review snapshots.
	--stress # Run the matching tests repeatedly until one fails.
	--stress-count: int # Run the matching tests this many times, then stop. Implies --stress.
	--tangram-path: path # Path to a prebuilt tangram binary to use instead of cargo build.
	--timeout: duration = 2min # The timeout for each test.
	--turso # Use Turso for the server database.
	--vfs # Run every test against the VFS: FSKit on macOS and FUSE on Linux. On macOS, this requires the app and its file system extension to be installed and enabled.
	--vm # Use vm isolation as the default for the test harness.
	...filters: string # Filter tests.
] {
	# Run the databases in a separate terminal.
	if $databases {
		if $nu.os-info.name not-in ['linux', 'macos'] {
			error make { msg: '--databases is supported on Linux and macOS only' }
		}
		let scylla_client_path = if $release { build_scylla_client --release } else { build_scylla_client }
		path add ($scylla_client_path | path dirname)
		let database_pool_workers = sys cpu | length
		let database_pool_size = $database_pool_workers * 4
		run_databases $database_pool_workers $database_pool_size

		return
	}

	# Validate the flags that affect the build.
	let cloud = $nu.os-info.name in ['linux', 'macos'] and not $no_cloud
	if $release and $tangram_path != null {
		error make { msg: '--release may not be combined with --tangram-path' }
	}
	if $tangram_path != null and not ($tangram_path | path exists) {
		error make { msg: $'--tangram-path does not exist: ($tangram_path)' }
	}
	if $kernel_path != null and not $vm {
		error make { msg: '--kernel-path may only be set with --vm' }
	}
	if $kernel_path != null and not ($kernel_path | path exists) {
		error make { msg: $'--kernel-path does not exist: ($kernel_path)' }
	}
	let fskit = $vfs and $nu.os-info.name == 'macos'

	# Determine which language clients the selected tests require.
	let filter = if ($filters | is-empty) {
		'.*'
	} else {
		$filters | each { '(' + $in + ')' } | str join '|'
	}
	let tests_path = $repository_path | path join 'packages/cli/tests'
	let names = if $clean { [] } else {
		fd -e nu -p $filter $tests_path | lines | each { |path| $path | path relative-to $tests_path }
	}
	let node = $names | any { |name| $name | str starts-with 'node/' }
	let python = $names | any { |name| $name | str starts-with 'python/' }

	# Build and install the current macOS app and file system extension. Isolate its default-feature Cargo build from the all-features test binary.
	if $fskit and not $clean {
		force_unmount_vfs_macos (fskit_temp_root)
		stop_fskit_provider $release
		let build_args = if $release { ['--release'] } else { [] }
		let cargo_target_dir = ($repository_path | path join 'target/macos')
		^bun run macos:build --cargo-target-dir $cargo_target_dir ...$build_args
		^nu ($repository_path | path join 'scripts/macos/install.nu') ...$build_args --no-build
	}

	# Build the test runner with Tangram in one Cargo invocation, and add both to the path. With --tangram-path, use the prebuilt binary instead.
	let profile = if $release { 'release' } else { 'debug' }
	let profile_args = if $release { ['--release'] } else { [] }
	let packages = ['--package' 'tangram_cli_test']
	let packages = if $cloud or $clean { $packages | append ['--package' 'tangram_scylla_client'] } else { $packages }
	let packages = if $node { $packages | append ['--package' 'tangram_javascript_native'] } else { $packages }
	let target_path = $repository_path | path join 'target' $profile
	if $tangram_path != null {
		cargo build ...$profile_args ...$packages
		let tangram_path = $tangram_path | path expand
		let bin_path = $repository_path | path join 'target/test-bin'
		mkdir $bin_path
		ln -sf $tangram_path ($bin_path | path join 'tangram')
		ln -sf $tangram_path ($bin_path | path join 'tg')
		path add $target_path
		path add $bin_path
	} else {
		cargo build ...$profile_args --all-features --package tangram_cli ...$packages
		ln -sf tangram ($target_path | path join 'tg')
		path add $target_path
	}

	# Clean up the leftover test resources.
	if $clean {
		let release_args = if $release { ['--release'] } else { [] }
		exec tangram_cli_test clean ...$release_args
	}

	# Build the Node.js client.
	if $node {
		let client_profile = if $release { 'release' } else { 'dev' }
		let client_build_args = if $tangram_path == null { ['--skip-extension-build'] } else { [] }
		bun run --filter @tangramdotdev/client build --profile $client_profile ...$client_build_args
	}

	# Build the Python client with an installed Python 3.12 or later.
	if $python {
		let client_profile = if $release { 'release' } else { 'dev' }
		let python_executable = $env.TANGRAM_PYTHON? | default 'python3'
		^$python_executable packages/clients/python/build.py --profile $client_profile
	}

	# Run the tests.
	let args = [
		(if $accept { ['--accept'] } else { [] })
		(if $cloud { ['--cloud'] } else { [] })
		(if $fskit { ['--fskit'] } else { [] })
		(if $jobs != null { ['--jobs' ($jobs | into string)] } else { [] })
		(if $kernel_path != null { ['--kernel-path' ($kernel_path | path expand)] } else { [] })
		(if $no_capture { ['--no-capture'] } else { [] })
		(if $no_progress_details { ['--no-progress-details'] } else { [] })
		(if $offline { ['--offline'] } else { [] })
		(if $preserve_failing_temps { ['--preserve-failing-temps'] } else { [] })
		(if $preserve_temps { ['--preserve-temps'] } else { [] })
		(if $print_passing_test_output { ['--print-passing-test-output'] } else { [] })
		(if $quickjs { ['--quickjs'] } else { [] })
		(if $review { ['--review'] } else { [] })
		(if $stress { ['--stress'] } else { [] })
		(if $stress_count != null { ['--stress-count' ($stress_count | into string)] } else { [] })
		['--timeout' ($timeout / 1sec | math ceil | into string)]
		(if $turso { ['--turso'] } else { [] })
		(if $vfs { ['--vfs'] } else { [] })
		(if $vm { ['--vm'] } else { [] })
		['--']
		$filters
	] | flatten
	exec tangram_cli_test run ...$args
}

def build_scylla_client [--release] {
	let args = if $release { ['--release'] } else { [] }
	cargo build --package tangram_scylla_client ...$args
	let profile = if $release { 'release' } else { 'debug' }

	$repository_path | path join target $profile tangram_scylla_client
}

def check_docker [] {
	let output = (docker info --format '{{.ServerVersion}}' | complete)
	if $output.exit_code != 0 {
		error make {
			msg: 'the Docker daemon is not available'
			help: ($output.stderr | str trim)
		}
	}
}

def database_lock_command [] {
	if $nu.os-info.name == 'macos' { 'lockf' } else { 'flock' }
}

def database_pool_path [] {
	($nu.temp-dir? | default $nu.temp-path?) | path join $database_pool_directory_name | path expand
}

def database_supervisor [] {
	r#'
set -mu

platform=$1
state_path=$2
cluster_path=$3
database_pool_path=$4
foundationdb_container=$5
foundationdb_image=$6
scylla_container=$7
scylla_image=$8
database_pool_workers=$9
database_pool_size=${10}
postgres_schema_path=${11}
scylla_schema_path=${12}
database_pool_acquire_script=${13}
declare -a logs=()
declare -a names=()
declare -a pids=()

cleanup() {
	status=$?
	trap - EXIT HUP INT TERM
	if [ "$platform" = macos ]; then
		docker rm --force "$foundationdb_container" "$scylla_container" >/dev/null 2>&1 || true
	fi
	for pid in "${pids[@]}"; do
		kill -TERM -- "-$pid" 2>/dev/null || true
	done
	for ((attempt = 0; attempt < 100; attempt++)); do
		alive=false
		for pid in "${pids[@]}"; do
			if process_running "$pid"; then
				alive=true
			fi
		done
		if ! $alive; then
			break
		fi
		sleep 0.05
	done
	for pid in "${pids[@]}"; do
		kill -KILL -- "-$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	done
	if ! rm -rf -- "$state_path" "$database_pool_path" 2>/dev/null; then
		echo "warning: failed to remove temporary database state in $state_path" >&2
	fi
	exit "$status"
}

handle_signal() {
	exit 130
}

process_running() {
	stat=$(ps -o stat= -p "$1" 2>/dev/null | tr -d ' ')
	case "$stat" in
		""|Z*) return 1 ;;
		*) return 0 ;;
	esac
}

show_log() {
	index=$1
	log_path=${logs[$index]}
	if [ -s "$log_path" ]; then
		echo "last 40 lines from ${names[$index]} ($log_path):" >&2
		tail -n 40 -- "$log_path" | sed -u "s/^/[${names[$index]}] /" >&2
	fi
}

show_logs() {
	for index in "${!logs[@]}"; do
		show_log "$index"
	done
}

ensure_running() {
	for index in "${!pids[@]}"; do
		if ! process_running "${pids[$index]}"; then
			echo "${names[$index]} exited before all databases became ready" >&2
			show_log "$index"
			exit 1
		fi
	done
}

start() {
	name=$1
	shift
	log_path="$state_path/logs/$name.log"
	logs+=("$log_path")
	names+=("$name")
	(
		cd "$state_path"
		exec "$@"
	) >"$log_path" 2>&1 &
	pids+=("$!")
}

wait_for() {
	name=$1
	attempts=$2
	shift 2
	for ((attempt = 0; attempt < attempts; attempt++)); do
		if timeout --kill-after=1s 4 "$@" >/dev/null 2>&1; then
			echo "$name is ready" >&2
			return
		fi
		ensure_running
		sleep 0.1
	done
	echo "timed out waiting for $name" >&2
	show_logs
	exit 1
}

configure_foundationdb() {
	for ((attempt = 0; attempt < 600; attempt++)); do
		if foundationdb_cli 2 --exec 'status minimal' >/dev/null 2>&1; then
			echo 'FoundationDB is ready' >&2
			return
		fi
		if foundationdb_cli 6 --exec 'configure new single memory' >/dev/null 2>&1; then
			echo 'FoundationDB is configured' >&2
			return
		fi
		ensure_running
		sleep 0.1
	done
	echo 'timed out configuring FoundationDB' >&2
	show_logs
	exit 1
}

foundationdb_cli() {
	operation_timeout=$1
	shift
	if [ "$platform" = linux ]; then
		timeout --kill-after=1s "$operation_timeout" fdbcli -C "$cluster_path" "$@"
	else
		docker exec "$foundationdb_container" timeout --kill-after=1s "$operation_timeout" fdbcli "$@"
	fi
}

wait_for_exit() {
	while true; do
		for index in "${!pids[@]}"; do
			pid=${pids[$index]}
			if ! process_running "$pid"; then
				wait "$pid" 2>/dev/null
				return $?
			fi
		done
		sleep 0.1
	done
}

provision_database_pool() {
	echo "pre-provisioning $database_pool_size database pool instances..." >&2
	for ((batch_start = 0; batch_start < database_pool_size; batch_start += database_pool_workers)); do
		declare -a provision_pids=()
		batch_end=$((batch_start + database_pool_workers))
		if [ "$batch_end" -gt "$database_pool_size" ]; then
			batch_end=$database_pool_size
		fi
		for ((index = batch_start; index < batch_end; index++)); do
			TANGRAM_TEST_DATABASE_PROVISION_CONCURRENT=1 bash -c "$database_pool_acquire_script" _ "$database_pool_path" "$postgres_schema_path" "$scylla_schema_path" >/dev/null &
			provision_pids+=("$!")
		done
		failed=false
		for pid in "${provision_pids[@]}"; do
			if ! wait "$pid" 2>/dev/null; then
				failed=true
			fi
		done
		if $failed; then
			echo 'failed to pre-provision the database pool' >&2
			exit 1
		fi
	done
	find "$database_pool_path" -mindepth 2 -maxdepth 2 -type f -name lease -delete
	actual_size=$(find "$database_pool_path" -mindepth 1 -maxdepth 1 -type d -name 'pool[0-9]*' | wc -l | tr -d ' ')
	if [ "$actual_size" -ne "$database_pool_size" ]; then
		echo "expected $database_pool_size database pool instances, found $actual_size" >&2
		exit 1
	fi
	echo "database pool is ready with $actual_size instances" >&2
}

trap cleanup EXIT
trap handle_signal HUP INT TERM

mkdir -p \
	"$state_path/logs" \
	"$state_path/postgres"

if [ "$platform" = linux ]; then
	mkdir -p \
		"$state_path/fdb/data" \
		"$state_path/fdb/logs" \
		"$state_path/scylla"
else
	docker rm --force "$foundationdb_container" "$scylla_container" >/dev/null 2>&1 || true
fi

initdb --auth=trust --no-instructions --pgdata="$state_path/postgres" --username=postgres >/dev/null
start postgres \
	postgres \
	-D "$state_path/postgres" \
	-h 127.0.0.1 \
	-k '' \
	-p 5432 \
	-c fsync=off \
	-c full_page_writes=off \
	-c max_connections=1024 \
	-c synchronous_commit=off
start nats \
	nats-server \
	--addr=127.0.0.1 \
	--port=4222

if [ "$platform" = linux ]; then
	start foundationdb \
		fdbserver \
		--cluster-file="$cluster_path" \
		--datadir="$state_path/fdb/data" \
		--listen-address=127.0.0.1:4500 \
		--logdir="$state_path/fdb/logs" \
		--public-address=127.0.0.1:4500
	start scylla \
		scylla \
		--api-address=127.0.0.1 \
		--default-log-level=warn \
		--developer-mode=1 \
		--kernel-page-cache=1 \
		--listen-address=127.0.0.1 \
		--max-partition-key-restrictions-per-query=1024 \
		--options-file="$state_path/scylla.yaml" \
		--overprovisioned \
		--rpc-address=127.0.0.1 \
		--unsafe-bypass-fsync=1 \
		--workdir="$state_path/scylla"
else
	start foundationdb \
		docker run \
		--rm \
		--name "$foundationdb_container" \
		--publish 127.0.0.1:4500:4500 \
		--env FDB_NETWORKING_MODE=host \
		--env FDB_PORT=4500 \
		--tmpfs /var/fdb/data:rw \
		--tmpfs /var/fdb/logs:rw \
		"$foundationdb_image"
	start scylla \
		docker run \
		--rm \
		--name "$scylla_container" \
		--publish 127.0.0.1:9042:9042 \
		--tmpfs /var/lib/scylla:rw,mode=0777 \
		"$scylla_image" \
		--auto-snapshot=false \
		--commitlog-segment-size-in-mb=8 \
		--commitlog-total-space-in-mb=1024 \
		--commitlog-use-hard-size-limit=true \
		--default-log-level=warn \
		--developer-mode=1 \
		--kernel-page-cache=1 \
		--max-partition-key-restrictions-per-query=1024 \
		--memory=1280MiB \
		--overprovisioned=1 \
		--schema-commitlog-segment-size-in-mb=32 \
		--smp=1 \
		--unsafe-bypass-fsync=1 \
		--write-request-timeout-in-ms=20000
fi

# Preserve the database process groups without printing job notifications.
set +m

configure_foundationdb
wait_for NATS 600 bash -c 'exec 3<>/dev/tcp/127.0.0.1/4222; IFS= read -r line <&3; case "$line" in INFO*) exit 0;; *) exit 1;; esac'
wait_for PostgreSQL 600 pg_isready --host=127.0.0.1 --port=5432 --username=postgres
wait_for ScyllaDB 1200 tangram_scylla_client 127.0.0.1 9042 -e 'select release_version from system.local'
provision_database_pool

echo 'all cloud databases are ready; press Ctrl-C to stop them' >&2

set +e
wait_for_exit
status=$?
set -e
echo 'a database process exited; stopping the remaining databases' >&2
for index in "${!pids[@]}"; do
	if ! process_running "${pids[$index]}"; then
		show_log "$index"
	fi
done
if [ "$status" -eq 0 ]; then
	status=1
fi
exit "$status"
'#
}

def ensure_docker_image [image: string] {
	let output = (docker image inspect $image | complete)
	if $output.exit_code != 0 {
		print -e $"pulling ($image)..."
		docker pull $image
	}
}

def fskit_provider_pids [executable: path] {
	ps --long
	| where { |process|
		($process.command? | default '') | str starts-with $executable
	}
	| get pid
}

def fskit_temp_root [] {
	$env.HOME | path join '.tangram/test-tmp'
}

# Unmounts any fskit vfs left under the path. A server that exits cleanly unmounts itself, so this only catches the ones that crashed.
def force_unmount_vfs_macos [path: string] {
	let targets = (
		try {
				^timeout 5 mount | lines | each { |line|
				let matches = ($line | parse --regex '^.+ on (?<target>.+) \(tangram[,)]')
				if ($matches | is-empty) { null } else { $matches | first | get target }
			} | compact
		} catch {
			[]
		}
	)
	let store_paths = (
		$targets
		| where { |target| ($target == ($path | path join 'store')) or ($target | str starts-with ($path + '/')) }
		| uniq
		| each { |path| { path: $path, length: ($path | str length) } }
		| sort-by length --reverse
		| get path
	)
	for store_path in $store_paths {
		let output = (^timeout --kill-after 2s 5 umount -f $store_path | complete)
		if $output.exit_code != 0 {
			error make { msg: $'failed to unmount the test VFS: ($store_path)', help: ($output.stderr | str trim) }
		}
	}
}

def run_databases [database_pool_workers: int, database_pool_size: int] {
	# Check the required programs.
	let lock_command = database_lock_command
	let commands = if $nu.os-info.name == 'linux' {
		[bash createdb dropdb fdbcli fdbserver initdb $lock_command nats-server pg_isready postgres psql scylla sed tail tangram_scylla_client timeout]
	} else {
		[bash createdb docker dropdb initdb $lock_command nats-server pg_isready postgres psql sed tail tangram_scylla_client timeout]
	}
	let missing = $commands | where { |command| which $command | is-empty }
	if not ($missing | is-empty) {
		error make {
			msg: $"the following database programs are missing from PATH: ($missing | str join ', ')"
			help: 'install the missing programs, then run this command again'
		}
	}
	if $nu.os-info.name == 'macos' {
		check_docker
	}

	# Check that the database ports are available.
	let endpoints = [
		{ name: PostgreSQL, port: 5432 },
		{ name: FoundationDB, port: 4500 },
		{ name: NATS, port: 4222 },
		{ name: ScyllaDB, port: 9042 },
	]
	let occupied = $endpoints | where { |endpoint| tcp_port_open $endpoint.port }
	if not ($occupied | is-empty) {
		let addresses = $occupied | each { |endpoint| $"($endpoint.name) at 127.0.0.1:($endpoint.port)" } | str join ', '
		error make {
			msg: $"the following database endpoints are already in use: ($addresses)"
			help: 'stop the existing services or processes, then run this command again'
		}
	}

	# Ensure the Docker images are available on macOS.
	if $nu.os-info.name == 'macos' {
		ensure_docker_image $foundationdb_image
		ensure_docker_image $scylla_image
	}

	# Create the temporary state.
	let state_path = if $nu.os-info.name == 'linux' {
		mktemp -d --tmpdir-path /var/tmp tangram_databases_XXXXXX | path expand
	} else {
		mktemp -d -t tangram_databases_XXXXXX | path expand
	}
	let cluster_path = $state_path | path join 'fdb.cluster'
	let database_pool_path = database_pool_path
	let postgres_schema_path = $repository_path | path join packages/server/src/database/postgres.sql
	let scylla_schema_path = $repository_path | path join packages/cache/scylla/schema.cql
	if ($database_pool_path | path exists) {
		rm -rf $database_pool_path
	}
	mkdir $database_pool_path
	foundationdb_cluster_description | save -f $cluster_path
	if $nu.os-info.name == 'linux' {
		[
			'# Test resets truncate tables frequently, so snapshots would grow for the lifetime of the suite.'
			'auto_snapshot: false'
			'cluster_name: tangram_test'
			'# Keep the global commitlog allocation bounded without reducing the number of Scylla shards.'
			'commitlog_segment_size_in_mb: 8'
			'commitlog_total_space_in_mb: 1024'
			'commitlog_use_hard_size_limit: true'
			'schema_commitlog_segment_size_in_mb: 32'
			'write_request_timeout_in_ms: 20000'
		] | str join "\n" | save -f ($state_path | path join 'scylla.yaml')
	}

	print -e $"starting the cloud databases with temporary state in ($state_path)"
	print -e $"database logs will be written to ($state_path | path join 'logs')"
	print -e 'waiting for FoundationDB, NATS, PostgreSQL, and ScyllaDB to become ready...'

	# Run the supervisor in the foreground.
	exec bash -c (database_supervisor) _ $nu.os-info.name $state_path $cluster_path $database_pool_path $foundationdb_container_name $foundationdb_image $scylla_container_name $scylla_image $database_pool_workers $database_pool_size $postgres_schema_path $scylla_schema_path (database_pool_acquire)
}

def stop_fskit_provider [release: bool] {
	let app_name = if $release { 'Tangram' } else { 'Tangram Dev' }
	let executable = (
		$env.HOME
		| path join $'Applications/($app_name).app/Contents/Extensions/TangramFSKit.appex/Contents/MacOS/TangramFSKit'
	)
	let pids = fskit_provider_pids $executable
	for pid in $pids {
		try { kill --quiet $pid }
	}
	for _ in 1..100 {
		if (fskit_provider_pids $executable | is-empty) {
			return
		}
		sleep 50ms
	}
	for pid in (fskit_provider_pids $executable) {
		try { kill --force --quiet $pid }
	}
}

def tcp_port_open [port: int] {
	(^bash -c 'exec 3<>/dev/tcp/127.0.0.1/"$1"' _ $port | complete).exit_code == 0
}
