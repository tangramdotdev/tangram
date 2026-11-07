#!/usr/bin/env nu

use std/util 'path add'

use util/test.nu [foundationdb_cluster_description]

const repository_path = path self '../../'
const database_pool_directory_name = 'tangram_test_database_pool'
const foundationdb_container_name = 'tangram_test_foundationdb'
const foundationdb_image = 'foundationdb/foundationdb:7.3.68'
const scylla_container_name = 'tangram_test_scylla'
const scylla_image = 'scylladb/scylla:2026.3.0'

# Run the shared cloud databases for the CLI tests in the foreground. This is supported on Linux and macOS.
def main [
	--release # Use a release build of the Scylla client.
] {
	if $nu.os-info.name not-in ['linux', 'macos'] {
		error make { msg: 'the cloud databases are supported on Linux and macOS only' }
	}
	let profile = if $release { 'release' } else { 'debug' }
	let target_path = $env.CARGO_TARGET_DIR? | default ($repository_path | path join 'target') | path expand
	path add ($target_path | path join $profile)
	run_databases $profile $target_path
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
repository_path=$9
profile=${10}
target_path=${11}
started_at=$SECONDS
build_pid=''
progress_pid=''
interrupted=false
starting=false
declare -a logs=()
declare -a names=()
declare -a pids=()

cleanup() {
	status=$?
	trap - EXIT
	# Finish the cleanup even if another interrupt arrives.
	trap '' HUP INT TERM
	interrupted=false
	finish_progress
	if [ -n "$build_pid" ]; then
		names+=(scylla_client)
		pids+=("$build_pid")
	fi
	start_progress stopping "${names[@]}"
	for pid in "${pids[@]}"; do
		kill -TERM -- "-$pid" 2>/dev/null || true
	done
	for ((attempt = 0; attempt < 100; attempt++)); do
		alive=false
		for index in "${!pids[@]}"; do
			if process_running "${pids[$index]}"; then
				alive=true
			else
				stopped "$index"
			fi
		done
		if ! $alive; then
			break
		fi
		sleep 0.05
	done
	for index in "${!pids[@]}"; do
		pid=${pids[$index]}
		kill -KILL -- "-$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
		stopped "$index"
	done
	# Remove the containers concurrently after their clients can no longer start them.
	if [ "$platform" = macos ]; then
		remove_container foundationdb "$foundationdb_container" &
		foundationdb_cleanup_pid=$!
		remove_container scylla "$scylla_container" &
		scylla_cleanup_pid=$!
		wait "$foundationdb_cleanup_pid" || true
		wait "$scylla_cleanup_pid" || true
	fi
	finish_progress
	for name in foundationdb scylla; do
		if [ -f "$progress_path/$name" ] && [[ $(<"$progress_path/$name") == failed* ]]; then
			cat "$state_path/logs/$name-cleanup.log" >&2
		fi
	done
	if ! rm -rf -- "$state_path" "$database_pool_path" 2>/dev/null; then
		echo "warning: failed to remove temporary database state in $state_path" >&2
	fi
	exit "$status"
}

stopped() {
	name=${names[$1]}
	if [ "$platform" = macos ] && { [ "$name" = foundationdb ] || [ "$name" = scylla ]; }; then
		return
	fi
	progress_result "$name" success
}

remove_container() {
	name=$1
	container=$2
	if docker rm --force "$container" >"$state_path/logs/$name-cleanup.log" 2>&1; then
		progress_result "$name" success
	elif remaining=$(docker container ls --all --filter "name=^/$container$" --format '{{.Names}}' 2>>"$state_path/logs/$name-cleanup.log") && [ -z "$remaining" ]; then
		progress_result "$name" success
	else
		progress_result "$name" failed
	fi
}

start_progress() {
	progress_phase=$1
	shift
	if [ "$#" -eq 0 ]; then
		return
	fi
	progress_path="$state_path/progress/$progress_phase"
	progress_started_at=$SECONDS
	mkdir -p "$progress_path"
	starting=true
	display_progress "$@" &
	progress_pid=$!
	starting=false
	if $interrupted; then
		exit 130
	fi
}

finish_progress() {
	if [ -z "$progress_pid" ]; then
		return
	fi
	: >"$progress_path/finished"
	wait "$progress_pid" 2>/dev/null || true
	if [ -t 2 ] && [ "${TERM:-dumb}" != dumb ]; then
		printf '\033[?2026h\r\033[J\033[?25h\033[?2026l' >&2
	fi
	progress_pid=''
}

progress_result() {
	if [ -f "$progress_path/$1" ] && { [ "$2" != failed ] || [[ $(<"$progress_path/$1") == failed* ]]; }; then
		return
	fi
	# Publish the complete result before the renderer reads it.
	printf '%s %s\n' "$2" "$((SECONDS - progress_started_at))" >"$progress_path/$1.tmp"
	mv -f -- "$progress_path/$1.tmp" "$progress_path/$1"
}

progress_name() {
	case "$1" in
		foundationdb) printf FoundationDB ;;
		nats) printf NATS ;;
		postgres) printf PostgreSQL ;;
		scylla) printf ScyllaDB ;;
		scylla_client) printf 'Scylla client' ;;
	esac
}

display_progress() {
	trap - EXIT
	trap '' HUP INT TERM
	local keys=("$@")
	local reported=()
	local live=false blue='' green='' red='' reset=''
	local frame running_frame key name result elapsed index completed failed running filled bar summary text ready state
	local columns rows width capacity lines size finished
	if [ -t 2 ] && [ "${TERM:-dumb}" != dumb ]; then
		live=true
		if [ -z "${NO_COLOR+x}" ]; then
			blue=$'\033[34m'
			green=$'\033[32m'
			red=$'\033[31m'
			reset=$'\033[0m'
		fi
		printf '\033[?25l' >&2
	fi
	# Render the results above the live region in a synchronized update every 100 ms.
	while true; do
		frame=''
		running_frame=''
		completed=0
		failed=0
		lines=0
		finished=false
		[ ! -f "$progress_path/finished" ] || finished=true
		columns=80
		rows=24
		if $live; then
			size=$(stty size <&2 2>/dev/null) && read -r rows columns <<<"$size"
			frame=$'\033[?2026h\r'
		fi
		width=$((columns > 1 ? columns - 1 : 1))
		capacity=$((rows > 2 ? rows - 2 : 0))
		for index in "${!keys[@]}"; do
			key=${keys[$index]}
			name=$(progress_name "$key")
			if [ -f "$progress_path/$key" ]; then
				read -r result elapsed <"$progress_path/$key"
				completed=$((completed + 1))
				if [ "$result" = failed ]; then
					failed=$((failed + 1))
				fi
				if [ "${reported[$index]:-}" != "$result" ]; then
					reported[$index]=$result
					if [ "$result" = failed ]; then
						frame+="${red}✗$reset $name failed ${elapsed}s"
					else
						result=ready
						[ "$progress_phase" != stopping ] || result=stopped
						frame+="${green}✓$reset $name $result ${elapsed}s"
					fi
					if $live; then
						frame+=$'\033[K'
					fi
					frame+=$'\n'
				fi
			elif $live && ! $finished && [ "$lines" -lt "$capacity" ]; then
				text="$name $progress_phase $((SECONDS - progress_started_at))s"
				if [ "${#text}" -gt "$((width - 2))" ]; then
					text="${text:0:$((width > 3 ? width - 3 : 0))}…"
				fi
				running_frame+="${blue}●$reset $text"$'\033[K\n'
				lines=$((lines + 1))
			fi
		done
		if $live && ! $finished; then
			running=$((${#keys[@]} - completed))
			filled=$((completed * 10 / ${#keys[@]}))
			printf -v bar '%*s' "$filled" ''
			bar=${bar// /=}
			printf -v text '%*s' "$((10 - filled))" ''
			bar+=">$text"
			ready=$((completed - failed))
			state=ready
			[ "$progress_phase" != stopping ] || state=stopped
			summary="[$bar] $completed/${#keys[@]}: $running running, $ready $state, $failed failed, $((SECONDS - progress_started_at))s"
			if [ "${#summary}" -gt "$width" ]; then
				summary="${summary:0:$((width - 1))}…"
			fi
			summary=${summary/"$ready $state"/"$green$ready $state$reset"}
			summary=${summary/"$failed failed"/"$red$failed failed$reset"}
			frame+="$running_frame$summary"$'\033[K\033[J\r'
			if [ "$lines" -gt 0 ]; then
				frame+=$'\033['"$lines"A
			fi
		elif $live; then
			frame+=$'\033[J'
		fi
		if $live; then
			frame+=$'\033[?2026l'
		fi
		printf '%s' "$frame" >&2
		if $finished; then
			break
		fi
		sleep 0.1
	done
	exit 0
}

handle_signal() {
	if $starting; then
		interrupted=true
		return
	fi
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
			progress_result "${names[$index]}" failed
			finish_progress
			echo "${names[$index]} exited before all databases became ready" >&2
			show_log "$index"
			exit 1
		fi
	done
}

start() {
	# Defer signals until the new process group is registered for cleanup.
	starting=true
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
	starting=false
	if $interrupted; then
		exit 130
	fi
}

wait_for() {
	name=$1
	attempts=$2
	shift 2
	for ((attempt = 0; attempt < attempts; attempt++)); do
		if timeout --kill-after=1s 4 "$@" >/dev/null 2>&1; then
			progress_result "$name" success
			return
		fi
		ensure_running
		sleep 0.1
	done
	progress_result "$name" failed
	finish_progress
	echo "timed out waiting for $name" >&2
	show_logs
	exit 1
}

configure_foundationdb() {
	configured=false
	for ((attempt = 0; attempt < 600; attempt++)); do
		if ! $configured && foundationdb_cli 6 --no-status --exec 'configure new single memory' >/dev/null 2>&1; then
			configured=true
		fi
		if $configured && output=$(foundationdb_cli 2 --no-status --exec 'status minimal' 2>/dev/null); then
			case "$output" in
				*'The database is available.'*)
					progress_result foundationdb success
					return
					;;
			esac
		fi
		ensure_running
		sleep 0.1
	done
	progress_result foundationdb failed
	finish_progress
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

trap cleanup EXIT
trap handle_signal HUP INT TERM

mkdir -p \
	"$state_path/logs" \
	"$state_path/postgres"

start_progress starting foundationdb nats postgres scylla_client scylla

# Build the Scylla client while the database services start.
build_args=()
if [ "$profile" = release ]; then
	build_args+=(--release)
fi
starting=true
(
	cd "$repository_path" || exit 1
	cargo build --package tangram_scylla_client --target-dir "$target_path" "${build_args[@]}" >"$state_path/logs/scylla_client.log" 2>&1 || exit $?
	progress_result scylla_client success
) &
build_pid=$!
starting=false
if $interrupted; then
	exit 130
fi

if [ "$platform" = linux ]; then
	mkdir -p \
		"$state_path/fdb/data" \
		"$state_path/fdb/logs" \
		"$state_path/scylla"
else
	docker rm --force "$foundationdb_container" "$scylla_container" >/dev/null 2>&1 || true
fi

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

initdb --auth=trust --no-instructions --no-sync --pgdata="$state_path/postgres" --username=postgres >"$state_path/logs/initdb.log" 2>&1 || {
	progress_result postgres failed
	finish_progress
	cat "$state_path/logs/initdb.log" >&2
	exit 1
}
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

# Preserve the database process groups without printing job notifications.
set +m

configure_foundationdb
wait_for nats 600 bash -c 'exec 3<>/dev/tcp/127.0.0.1/4222; IFS= read -r line <&3; case "$line" in INFO*) exit 0;; *) exit 1;; esac'
wait_for postgres 600 pg_isready --host=127.0.0.1 --port=5432 --username=postgres
while process_running "$build_pid"; do
	ensure_running
	sleep 0.1
done
if ! wait "$build_pid"; then
	progress_result scylla_client failed
	finish_progress
	echo 'failed to build the Scylla client' >&2
	cat "$state_path/logs/scylla_client.log" >&2
	exit 1
fi
build_pid=''
wait_for scylla 1200 tangram_scylla_client 127.0.0.1 9042 -e 'select release_version from system.local'

finish_progress
echo "all cloud databases are ready ($((SECONDS - started_at))s since startup); press Ctrl-C to stop them" >&2

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

def run_databases [profile: string, target_path: string] {
	# Check the required programs.
	let lock_command = database_lock_command
	let commands = if $nu.os-info.name == 'linux' {
		[bash cargo cat createdb dropdb fdbcli fdbserver initdb $lock_command nats-server pg_isready postgres psql scylla sed tail timeout]
	} else {
		[bash cargo cat createdb docker dropdb initdb $lock_command nats-server pg_isready postgres psql sed tail timeout]
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

	# Run the supervisor in the foreground.
	exec bash -c (database_supervisor) _ $nu.os-info.name $state_path $cluster_path $database_pool_path $foundationdb_container_name $foundationdb_image $scylla_container_name $scylla_image $repository_path $profile $target_path
}

def tcp_port_open [port: int] {
	(^bash -c 'exec 3<>/dev/tcp/127.0.0.1/"$1"' _ $port | complete).exit_code == 0
}
