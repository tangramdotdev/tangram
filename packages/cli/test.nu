#!/usr/bin/env nu

use std/util 'path add'
use ../../scripts/macos/identifiers.nu

export use std/assert

const repository_path = path self '../../'
const harness_path = path self
const database_pool_directory_name = 'tangram_test_database_pool'
const database_reset_attempts = 2
const database_reset_timeout = 30sec
const foundationdb_container_name = 'tangram_test_foundationdb'
const foundationdb_image = 'foundationdb/foundationdb:7.3.68'
const scylla_container_name = 'tangram_test_scylla'
const scylla_image = 'scylladb/scylla:2026.1.1'
const server_exit_directory_name = 'server_jobs'
const worker_cleanup_grace = 75sec
const worker_exit_grace = 2sec

def main [
	--accept (-a) # Accept all new and updated snapshots.
	--clean # Clean up leftover test resources from FoundationDB, PostgreSQL, and ScyllaDB.
	--databases # Run the shared cloud databases in the foreground. This is supported on Linux and macOS.
	--jobs (-j): int # The number of concurrent tests to run.
	--kernel-path: path # The path to the linux kernel image to use with --vm. Required when --vm is set.
	--no-cloud # Use local backends for test instances.
	--preserve-temps # Keep the temporary directories.
	--no-capture # Do not capture the output of each test. This sets --jobs to 1.
	--no-progress-details # Show only the aggregate progress bar, without listing running tests.
	--offline # Skip tests which require network access.
	--print-passing-test-output # Print the output of passing tests.
	--quickjs # Use QuickJS as the JS engine.
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

	let cloud = $nu.os-info.name in ['linux', 'macos'] and not $no_cloud
	if $jobs != null and $jobs < 1 {
		error make { msg: '--jobs must be at least one' }
	}
	if $timeout <= 0sec {
		error make { msg: '--timeout must be greater than zero' }
	}

	# Validate the --vm/--kernel-path flag combination.
	if $vm and $kernel_path == null {
		error make { msg: '--kernel-path is required when --vm is set' }
	}
	if $kernel_path != null and not $vm {
		error make { msg: '--kernel-path may only be set with --vm' }
	}
	if $kernel_path != null and not ($kernel_path | path exists) {
		error make { msg: $'--kernel-path does not exist: ($kernel_path)' }
	}
	# Validate the release flag combination.
	if $release and $tangram_path != null {
		error make { msg: '--release may not be combined with --tangram-path' }
	}
	# Use FSKit for the VFS on macOS.
	let fskit = $vfs and $nu.os-info.name == 'macos'
	# Validate the stress flag combination.
	let stress = $stress or $stress_count != null
	if $stress and ($accept or $review) {
		error make { msg: '--stress may not be combined with --accept or --review' }
	}
	if $stress_count != null and $stress_count < 1 {
		error make { msg: '--stress-count must be at least one' }
	}
	# Clean up leftover test resources if requested.
	if $clean {
		let scylla_client_path = if $release { build_scylla_client --release } else { build_scylla_client }
		path add ($scylla_client_path | path dirname)

		let fskit_temp_paths = if (fskit_temp_root | path exists) {
			ls (fskit_temp_root) | where name =~ 'tangram_test_' and type == dir | get name
		} else {
			[]
		}
		let test_temp_paths = (
			ls ($nu.temp-dir? | default $nu.temp-path?)
			| where name =~ 'tangram_test_' and type == dir
			| get name
			| append $fskit_temp_paths
		)
		let lmdb_sysv_keys = lmdb_sysv_keys_for_test_dirs $test_temp_paths

		for path in $test_temp_paths {
			remove_temp_directory $path
			print -e $"removed ($path)"
		}

		let postgres_output = (^timeout 5 psql --host=127.0.0.1 --username=postgres --dbname=postgres --tuples-only --no-align --command 'select datname from pg_database' | complete)
		if $postgres_output.exit_code == 0 {
			let databases = $postgres_output.stdout | lines | str trim | where { $in starts-with 'database_' }
			for database in $databases {
				print -e $"dropping PostgreSQL database ($database)"
				try { ^dropdb --host=127.0.0.1 --username=postgres --if-exists --force $database }
			}
		} else {
			print -e 'skipping PostgreSQL cleanup because it is not ready'
		}

		let scylla_output = (^timeout 5 tangram_scylla_client 127.0.0.1 9042 -e "SELECT JSON keyspace_name FROM system_schema.keyspaces" | complete)
		if $scylla_output.exit_code == 0 {
			let keyspaces = $scylla_output.stdout | lines | str trim | where { $in starts-with '{' } | each { $in | from json | get keyspace_name } | where { $in starts-with 'store_' }
			for keyspace in $keyspaces {
				print -e $"dropping scylla keyspace ($keyspace)"
				try { tangram_scylla_client 127.0.0.1 9042 -e $"drop keyspace \"($keyspace)\";" e> /dev/null }
			}
		} else {
			print -e 'skipping ScyllaDB cleanup because it is not ready'
		}

		let foundationdb_command = foundationdb_command
		let foundationdb_output = (^timeout 10 ...$foundationdb_command --exec 'writemode on; clearrange "" \xff' | complete)
		if $foundationdb_output.exit_code == 0 {
			print -e 'cleared FoundationDB test data'
		} else {
			print -e 'skipping FoundationDB cleanup because it is not ready'
		}

		let database_pool_path = database_pool_path
		if ($database_pool_path | path exists) {
			rm -rf $database_pool_path
		}
		mkdir $database_pool_path

		let tangram_processes = count_tangram_processes
		if $tangram_processes > 0 {
			clean_tangram_processes
		}
		let remaining_tangram_processes = count_tangram_processes
		print -e $"cleaned tangram processes: ($tangram_processes - $remaining_tangram_processes)"

		let lmdb_sysv_semaphores = count_lmdb_sysv_semaphores $lmdb_sysv_keys
		if $lmdb_sysv_semaphores > 0 {
			clean_lmdb_sysv_semaphores $lmdb_sysv_keys
		}
		let remaining_lmdb_sysv_semaphores = count_lmdb_sysv_semaphores $lmdb_sysv_keys
		print -e $"cleaned lmdb sysv semaphores: ($lmdb_sysv_semaphores - $remaining_lmdb_sysv_semaphores)"

		return
	}

	# Build and install the current macOS app and file system extension. Isolate
	# its default-feature Cargo build from the all-features test binary.
	if $fskit {
		force_unmount_vfs (fskit_temp_root)
		stop_fskit_provider $release
		let build_args = if $release { ['--release'] } else { [] }
		let cargo_target_dir = ($repository_path | path join 'target/macos')
		^bun run macos:build --cargo-target-dir $cargo_target_dir ...$build_args
		^nu ($repository_path | path join 'scripts/macos/install.nu') ...$build_args --no-build
	}

	# Add the tangram binary to the path. If --tangram-path was provided, use
	# its parent directory directly and place the tg symlink in a temp dir;
	# otherwise build from source.
	if $tangram_path != null {
		if not ($tangram_path | path exists) {
			error make { msg: $'--tangram-path does not exist: ($tangram_path)' }
		}
		let tangram_path = $tangram_path | path expand
		let tg_dir = mktemp -d -t tangram_test_tg_XXXXXX
		ln -sf $tangram_path ($tg_dir | path join 'tg')
		if $cloud {
			let scylla_client_path = if $release { build_scylla_client --release } else { build_scylla_client }
			ln -sf $scylla_client_path ($tg_dir | path join 'tangram_scylla_client')
		}
		path add ($tangram_path | path dirname)
		path add $tg_dir
	} else if $release {
		if $cloud {
			cargo build --release --all-features --package tangram_cli --package tangram_scylla_client
		} else {
			cargo build --release --all-features
		}
		ln -sf tangram target/release/tg
		path add ($repository_path | path join 'target/release')
	} else {
		if $cloud {
			cargo build --all-features --package tangram_cli --package tangram_scylla_client
		} else {
			cargo build --all-features
		}
		ln -sf tangram target/debug/tg
		path add ($repository_path | path join 'target/debug')
	}

	# Build the Node.js client.
	bun run --filter @tangramdotdev/client build

	if $cloud {
		check_databases
		reclaim_stale_database_leases (database_pool_path)
	}
	let database_pool_path = if $cloud { database_pool_path } else { '' }

	# Get the matching tests.
	let filter = if ($filters | is-empty) {
		'.*'
	} else {
		$filters | each { '(' + $in + ')' } | str join '|'
	}
	let tests_path = ($repository_path | path join 'packages/cli/tests')
	let tests = fd -e nu -p $filter $tests_path | lines | sort | where { |path|
		not (($path | path relative-to $tests_path) | str starts-with 'lib/')
	} | each { |path|
		{
			path: $path,
			name: ($path | path relative-to $tests_path)
		}
	}
	if ($tests | is-empty) {
		error make { msg: 'no tests matched the provided filters' }
	}

	mut results = []

	# Determine the number of concurrent tests to run.
	let jobs = $jobs | default (sys cpu | length)
	let jobs = if $no_capture {
		1
	} else {
		$jobs
	}

	let options = {
		cloud: $cloud,
		database_pool_path: $database_pool_path,
		fskit: $fskit,
		kernel_path: ($kernel_path | default "" | into string),
		no_capture: $no_capture,
		offline: $offline,
		preserve_temps: $preserve_temps,
		quickjs: $quickjs,
		stress: $stress,
		timeout: $timeout,
		turso: $turso,
		vfs: $vfs,
		vm: $vm,
	}
	if $no_capture {
		mut round = 1
		mut stop = false
		while not $stop {
			for test in $tests {
				let result = run_test $test $options
				print_test_result $result $print_passing_test_output
				$results = $results | append $result
				if $stress and (is_failed $result) {
					print -e $'(ansi red)($result.name) failed on round ($round)(ansi reset)'
					$stop = true
					break
				}
			}
			if not $stress or ($stress_count != null and $round >= $stress_count) {
				$stop = true
			}
			$round = $round + 1
		}
	} else {
		# Create the state.
		mut pending = round_entries $tests 1 0
		mut next_seq = $tests | length
		mut round = 1
		mut stress_stopped = false
		mut running = []

		let start = date now
		let total = if $stress {
			if $stress_count != null { ($tests | length) * $stress_count } else { 0 }
		} else {
			$pending | length
		}
		let total_display = if $stress and $stress_count == null { '∞' } else { $total }

		def spawn [test: record] {
			job spawn {
				let start = date now
				let result = try {
					run_test $test $options
				} catch { |error|
					{
						cleanup_duration: null,
						duration: ((date now) - $start),
						execution_duration: null,
						name: $test.name,
						output: {
							exit_code: 1,
							stdout: '',
							stderr: ($error | get msg | default ($error | to nuon)),
						},
						temp_path: '',
					}
				}
				$result | merge { seq: $test.seq, round: $test.round } | job send 0
			}
		}

		# Spawn a job that sends a null message every second to trigger progress updates.
		let interval_job = job spawn {
			loop {
				sleep 1sec
				null | job send 0
			}
		}

		# Hide the cursor.
		print -e -n "\e[?25l"

		# Process results as they complete.
		while ($running | length) > 0 or ($pending | length) > 0 or ($stress and not $stress_stopped and ($stress_count == null or $round < $stress_count)) {
			# Keep the worker pool full. In stress mode, refill the queue with the next round as necessary, so the pool stays full even when fewer tests match than there are jobs.
			while ($running | length) < $jobs {
				if ($pending | is-empty) {
					if $stress and not $stress_stopped and ($stress_count == null or $round < $stress_count) {
						$round = $round + 1
						$pending = round_entries $tests $round $next_seq
						$next_seq = $next_seq + ($tests | length)
					} else {
						break
					}
				}
				let test = $pending | first
				$pending = $pending | skip 1
				let id = spawn $test
				$running = $running | append { id: $id, missing_since: null, name: $test.name, round: $test.round, seq: $test.seq, start: (date now) }
			}

			# Wait for the next event, then drain the mailbox before checking worker liveness. A Nu job exits immediately after sending its result, so checking the job table while completed results are still queued can falsely report dead workers.
			let result = job recv
			mut messages = [$result]
			loop {
				let message = try {
					{ received: true, value: (job recv --timeout 0sec) }
				} catch {
					{ received: false, value: null }
				}
				if not $message.received {
					break
				}
				$messages = $messages | append $message.value
			}

			# Clear the from the cursor to the end.
			print -e -n "\e[0J"

			for result in $messages {
				if $result == null or ($running | where seq == $result.seq | is-empty) {
					continue
				}
				print_test_result $result $print_passing_test_output

				# Store the result.
				$results = $results | append $result

				# Remove the completed job from the running list.
				$running = $running | where seq != $result.seq

				# In stress mode, stop spawning new tests after the first failure.
				if $stress and (is_failed $result) {
					print -e $'(ansi red)($result.name) failed on round ($result.round)(ansi reset)'
					$pending = []
					$stress_stopped = true
				}
			}

			# Reconcile the logical worker list with Nu's job table and enforce a hard worker deadline.
			let active_job_ids = job list | get id
			let now = date now
			mut next_running = []
			mut worker_failures = []
			for worker in $running {
				let duration = $now - $worker.start
				let deadline = $options.timeout + $worker_cleanup_grace
				if $duration > $deadline {
					try { job kill $worker.id }
					let message = $'the test worker exceeded its hard deadline of ($deadline)'
					$worker_failures = $worker_failures | append (worker_failure_result $worker $message)
				} else if $worker.id in $active_job_ids {
					$next_running = $next_running | append ($worker | upsert missing_since null)
				} else if $worker.missing_since == null {
					$next_running = $next_running | append ($worker | upsert missing_since $now)
				} else if ($now - $worker.missing_since) >= $worker_exit_grace {
					let message = 'the test worker exited without reporting a result'
					$worker_failures = $worker_failures | append (worker_failure_result $worker $message)
				} else {
					$next_running = $next_running | append $worker
				}
			}
			$running = $next_running
			for failure in $worker_failures {
				print_test_result $failure $print_passing_test_output
				$results = $results | append $failure
				if $stress {
					print -e $'(ansi red)($failure.name) failed on round ($failure.round)(ansi reset)'
					$pending = []
					$stress_stopped = true
				}
			}

			# Print the running tests unless the aggregate-only view was requested.
			if not $no_progress_details {
				let term_width = term size | get columns
				for test in $running {
					let duration = ((date now) - $test.start) / 1sec | math floor | into duration -u sec
					let text = $'($test.name) ($duration)'
					let max_length = $term_width - 2
					let text = if ($text | str length) > $max_length {
						($text | str substring ..($max_length - 2)) + '…'
					} else {
						$text
					}
					print -e $'(ansi blue)●(ansi reset) ($text)'
				}
			}

			# Print the progress bar.
			let completed = $results | length
			let passed = $results | where output.exit_code == 0 | length
			let skipped = $results | where output.exit_code == 77 | length
			let failed = $results | where { |result| is_failed $result } | length
			let ratio = if $total > 0 { $completed / $total } else { 0 }
			let filled = ($ratio * 10) | math floor
			let bar = if $filled > 0 { (1..$filled | each { '=' } | str join) + '>' } else { '>' }
			let bar = if $filled < 10 { $bar + (1..(10 - $filled) | each { ' ' } | str join) } else { $bar }
			let elapsed = ((date now) - $start) / 1sec | math floor | into duration -u sec
			let progress = $'[($bar)] ($completed)/($total_display): ($running | length) running, (ansi green)($passed) passed(ansi reset), (ansi yellow)($skipped) skipped(ansi reset), (ansi red)($failed) failed(ansi reset), ($elapsed)'
			print -e -n $'($progress)'

			# Move the cursor up.
			print -e -n $"\r"
			if not $no_progress_details and ($running | length) > 0 {
				print -e -n $"\e[($running | length)A"
			}
		}

		job kill $interval_job

		# Clear.
		print -e -n "\e[0J"

		# Show the cursor.
		print -e -n "\e[?25h"
	}

	if $accept {
		for test in $tests {
			let parsed = $test.path | path parse

			# Accept all pending file snapshots.
			for pending_path in (glob $'($parsed.parent | path join $parsed.stem){.{pending},/*.{pending}}') {
				let snapshot_path = $pending_path | str replace '.pending' '.snapshot'
				mv -f $pending_path $snapshot_path
			}

			# Accept all inline snapshots.
			let inline_paths = glob $'($parsed.parent | path join $parsed.stem).inline'
			for inline_path in $inline_paths {
				let entries = open $inline_path | from json
				let sorted_entries = $entries | sort-by position --reverse
				mut source = open $test.path
				for entry in $sorted_entries {
					let before = $source | str substring ..<$entry.position
					let indent = get_indent $source $entry.position
					let after = $source | str substring ($entry.position + $entry.length)..
					$source = $before ++ (literal $entry.new $indent) ++ $after
				}
				$source | save -f $test.path
				rm $inline_path
			}
		}
	}

	if $review {
		for test in $tests {
			let parsed = $test.path | path parse

			let pending_paths = glob $'($parsed.parent | path join $parsed.stem){.{pending},/*.{pending}}'
			for pending_path in $pending_paths {
				let snapshot_path = $pending_path | str replace '.pending' '.snapshot'
				clear -k
				if ($snapshot_path | path exists) {
					print -e $'(ansi yellow)changed(ansi reset) ($snapshot_path)'
					diff $snapshot_path $pending_path --path | print -e
				} else {
					print -e $'(ansi green)added(ansi reset) ($snapshot_path)'
					print -e ''
					print -e -n (ansi green)
					open $pending_path | print -e
					print -e (ansi reset)
				}
				print -e ''
				print -e -n $'(ansi green)[a]ccept(ansi reset) or (ansi red)[r]eject(ansi reset): '
				loop {
					let response = input -n 1 -s
					if $response == 'a' {
						mv -f $pending_path $snapshot_path
						break
					} else if $response == 'r' {
						rm $pending_path
						break
					}
				}
				print -e ''
			}

			let inline_paths = glob $'($parsed.parent | path join $parsed.stem).inline'
			for inline_path in $inline_paths {
				let entries = open $inline_path | from json
				for entry in $entries {
					clear -k
					print -e $'(ansi yellow)changed(ansi reset) ($test.path)'
					diff $entry.old $entry.new | print -e
					print -e ''
				}
				print -e -n $'(ansi green)[a]ccept(ansi reset) or (ansi red)[r]eject(ansi reset): '
				loop {
					let response = input -n 1 -s
					if $response == 'a' {
						let sorted_entries = $entries | sort-by position --reverse
						mut source = open $test.path
						for entry in $sorted_entries {
							let before = $source | str substring ..<$entry.position
							let indent = get_indent $source $entry.position
							let after = $source | str substring ($entry.position + $entry.length)..
							$source = $before ++ (literal $entry.new $indent) ++ $after
						}
						$source | save -f $test.path
					} else if $response != 'r' {
						continue
					}
					rm $inline_path
					break
				}
				print -e ''
			}

			# Delete snapshots which were not touched and remove touched files.
			if ($pending_paths | length) > 0 or ($inline_paths | length) > 0 {
				for path in (glob $'($parsed.parent | path join $parsed.stem){.snapshot,/*.snapshot}') {
					if not ($path | str replace '.snapshot' '.touched' | path exists) {
						rm $path
					}
				}
				for path in (glob $'($parsed.parent | path join $parsed.stem){.touched,/*.touched}') {
					rm $path
				}
			}
		}
	}

	# Print the summary.
	let passed = $results | where output.exit_code == 0 | length
	let skipped = $results | where output.exit_code == 77 | length
	let failed = $results | where { |result| is_failed $result }
	let total = $results | length
	print -e $'(ansi green)($passed) passed(ansi reset), (ansi yellow)($skipped) skipped(ansi reset), (ansi red)($failed | length) failed(ansi reset), ($total) total'

	# Print the failed tests.
	for result in $failed {
		let exit_code = $result.output.exit_code
		let description = exit_description $exit_code
		print -e $'(ansi red)✗(ansi reset) ($result.name) ($result.duration), exit ($exit_code) (($description))'
	}

	if $preserve_temps {
		print -e ''
		print -e 'preserved temp directories:'
		for result in $results {
			print -e $'  ($result.name): ($result.temp_path)'
		}
	}

	if not ($failed | is-empty) {
		exit 1
	}
}

def build_scylla_client [--release] {
	let args = if $release { ['--release'] } else { [] }
	cargo build --package tangram_scylla_client ...$args
	let profile = if $release { 'release' } else { 'debug' }

	$repository_path | path join target $profile tangram_scylla_client
}

def database_pool_path [] {
	($nu.temp-dir? | default $nu.temp-path?) | path join $database_pool_directory_name | path expand
}

def reclaim_stale_database_leases [pool_path: string] {
	let result = (^bash -c (database_pool_reclaim_stale_leases) _ $pool_path | complete)
	if $result.exit_code != 0 {
		error make {
			msg: 'failed to reclaim stale database pool leases'
			help: ($result.stderr | str trim)
		}
	}
	let count = $result.stdout | str trim | into int
	if $count > 0 {
		print -e $'reclaimed ($count) stale database pool leases'
	}
}

def database_pool_reclaim_stale_leases [] {
	r#'
set -euo pipefail

pool_path=$1
count=0
for lease_path in "$pool_path"/pool[0-9]*/lease; do
	if [ ! -f "$lease_path" ]; then
		continue
	fi
	owner_pid=$(sed -n '1p' "$lease_path")
	owner_start=$(sed -n '2p' "$lease_path")
	case "$owner_pid" in
		''|*[!0-9]*) owner_pid= ;;
	esac
	current_start=
	if [ -n "$owner_pid" ] && [ -n "$owner_start" ]; then
		current_start=$(ps -o lstart= -p "$owner_pid" 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' || true)
	fi
	if [ -n "$owner_pid" ] && [ -n "$owner_start" ] && [ "$current_start" = "$owner_start" ]; then
		continue
	fi
	if rm -f -- "$lease_path"; then
		count=$((count + 1))
	fi
done
printf '%s\n' "$count"
'#
}

def acquire_database_instance [pool_path: string] {
	let postgres_schema_path = $repository_path | path join packages/server/src/database/postgres.sql
	let scylla_schema_path = $repository_path | path join packages/store/src/scylla.cql
	let result = (^bash -c (database_pool_acquire) _ $pool_path $postgres_schema_path $scylla_schema_path | complete)
	if $result.exit_code != 0 {
		error make {
			msg: 'failed to acquire a database pool instance'
			help: ($result.stderr | str trim)
		}
	}
	let instance = $result.stdout | str trim
	if ($instance | is-empty) {
		error make { msg: 'the acquired database pool instance is empty' }
	}

	$instance
}

def database_pool_acquire [] {
	r#'
set -euo pipefail

pool_path=$1
postgres_schema_path=$2
scylla_schema_path=$3
operation_timeout=30
mkdir -p -- "$pool_path"

owner_pid=$PPID
owner_start=$(ps -o lstart= -p "$owner_pid" 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' || true)
if [ -z "$owner_start" ]; then
	echo "failed to identify the database lease owner process $owner_pid" >&2
	exit 1
fi

try_lease_existing() {
	for slot_path in "$pool_path"/pool[0-9]*; do
		if [ ! -d "$slot_path" ]; then
			continue
		fi
		temporary_lease_path=$(mktemp "$slot_path/.lease.XXXXXX")
		printf '%s\n%s\n' "$owner_pid" "$owner_start" > "$temporary_lease_path"
		if ln "$temporary_lease_path" "$slot_path/lease" 2>/dev/null; then
			rm -f -- "$temporary_lease_path"
			basename "$slot_path"
			return 0
		fi
		rm -f -- "$temporary_lease_path"
	done
	return 1
}

if instance=$(try_lease_existing); then
	echo "$instance"
	exit 0
fi

# Schema changes are coordinated globally by Scylla. Serialize only on-demand provisioning so a cold expansion cannot overwhelm it; the database operations themselves remain unrestricted.
if [ "${TANGRAM_TEST_DATABASE_PROVISION_CONCURRENT:-}" != 1 ]; then
	exec 9>"$pool_path/provision.lock"
	if [ "$(uname -s)" = Darwin ]; then
		lockf 9
	else
		flock 9
	fi
	if instance=$(try_lease_existing); then
		echo "$instance"
		exit 0
	fi
fi

index=0
while true; do
	instance=$(printf 'pool%04d' "$index")
	slot_path="$pool_path/$instance"
	temporary_slot_path="$pool_path/.provisioning.$instance"
	if [ ! -e "$slot_path" ] && mkdir "$temporary_slot_path" 2>/dev/null; then
		if [ ! -e "$slot_path" ]; then
			break
		fi
		rmdir "$temporary_slot_path"
	fi
	index=$((index + 1))
done
postgres_created=false
scylla_created=false

cleanup_provision() {
	status=$?
	trap - EXIT
	rm -rf -- "$temporary_slot_path"
	if $scylla_created; then
		timeout --kill-after=2s 10 tangram_scylla_client 127.0.0.1 9042 -e "drop keyspace if exists \"store_$instance\";" >/dev/null 2>&1 || true
	fi
	if $postgres_created; then
		timeout --kill-after=2s 10 dropdb --host=127.0.0.1 --username=postgres --if-exists --force "database_$instance" >/dev/null 2>&1 || true
	fi
	exit "$status"
}
trap cleanup_provision EXIT

printf '%s\n%s\n' "$owner_pid" "$owner_start" > "$temporary_slot_path/lease"
run_phase() {
	phase=$1
	shift
	"$@" || {
		status=$?
		echo "$phase failed with exit code $status" >&2
		return "$status"
	}
}

run_phase "creating PostgreSQL database $instance" timeout --kill-after=2s "$operation_timeout" createdb --host=127.0.0.1 --username=postgres "database_$instance"
postgres_created=true
run_phase "initializing PostgreSQL database $instance" timeout --kill-after=2s "$operation_timeout" psql --host=127.0.0.1 --username=postgres --dbname="database_$instance" --set=ON_ERROR_STOP=1 --single-transaction --file="$postgres_schema_path" >/dev/null

run_phase "creating ScyllaDB keyspace $instance" timeout --kill-after=2s "$operation_timeout" tangram_scylla_client 127.0.0.1 9042 -e "create keyspace \"store_$instance\" with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };" >/dev/null
scylla_created=true
run_phase "initializing ScyllaDB keyspace $instance" timeout --kill-after=2s "$operation_timeout" tangram_scylla_client 127.0.0.1 9042 -k "store_$instance" -f "$scylla_schema_path" >/dev/null

mv -- "$temporary_slot_path" "$slot_path"
trap - EXIT
echo "$instance"
'#
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
	let scylla_schema_path = $repository_path | path join packages/store/src/scylla.cql
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

def check_databases [] {
	# Check the client programs before attempting the readiness commands.
	let commands = if $nu.os-info.name == 'linux' {
		[bash createdb dropdb fdbcli pg_isready psql tangram_scylla_client timeout]
	} else {
		[bash createdb docker dropdb pg_isready psql tangram_scylla_client timeout]
	}
	let missing = $commands | where { |command| which $command | is-empty }
	if not ($missing | is-empty) {
		error make {
			msg: $"the following database clients are missing from PATH: ($missing | str join ', ')"
			help: 'install the database programs, then run `nu packages/cli/test.nu --databases` in another terminal'
		}
	}

	# Check each database independently so the error names every unavailable service.
	let foundationdb_command = foundationdb_command
	let postgres_ready = (^timeout 5 pg_isready --host=127.0.0.1 --port=5432 --username=postgres | complete).exit_code == 0
	let database_pool_ready = (database_pool_path) | path exists
	let fdb_ready = (^timeout --kill-after 2s 8 ...$foundationdb_command --exec 'status minimal' | complete).exit_code == 0
	let nats_ready = nats_ready
	let scylla_ready = (^timeout 5 tangram_scylla_client 127.0.0.1 9042 -e 'select release_version from system.local' | complete).exit_code == 0
	let unavailable = [
		{ name: PostgreSQL, ready: $postgres_ready },
		{ name: 'database pool', ready: $database_pool_ready },
		{ name: FoundationDB, ready: $fdb_ready },
		{ name: NATS, ready: $nats_ready },
		{ name: ScyllaDB, ready: $scylla_ready },
	] | where not ready | get name
	if not ($unavailable | is-empty) {
		error make {
			msg: $"the following cloud databases are not ready: ($unavailable | str join ', ')"
			help: 'run `nu packages/cli/test.nu --databases` in another terminal'
		}
	}
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

def foundationdb_command [operation_timeout: duration = 5sec] {
	let operation_timeout_secs = $operation_timeout | into int | $in / 1_000_000_000

	if $nu.os-info.name == 'linux' {
		[timeout --kill-after=2s $operation_timeout_secs fdbcli -C (fdb_cluster)]
	} else {
		[docker exec $foundationdb_container_name timeout --kill-after=2s $operation_timeout_secs fdbcli]
	}
}

def foundationdb_cluster_description [] {
	if $nu.os-info.name == 'linux' { 'local:local@127.0.0.1:4500' } else { 'docker:docker@127.0.0.1:4500' }
}

def ensure_docker_image [image: string] {
	let output = (docker image inspect $image | complete)
	if $output.exit_code != 0 {
		print -e $"pulling ($image)..."
		docker pull $image
	}
}

def tcp_port_open [port: int] {
	(^bash -c 'exec 3<>/dev/tcp/127.0.0.1/"$1"' _ $port | complete).exit_code == 0
}

def nats_ready [] {
	let command = 'exec 3<>/dev/tcp/127.0.0.1/4222; IFS= read -r line <&3; case "$line" in INFO*) exit 0;; *) exit 1;; esac'
	(^timeout 2 bash -c $command | complete).exit_code == 0
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

# Report whether a result represents a failure. Exit code 77 means the test was skipped.
def is_failed [result: record] {
	$result.output.exit_code != 0 and $result.output.exit_code != 77
}

# Create a synthetic failure when a scheduler worker cannot report its own result.
def worker_failure_result [worker: record, message: string] {
	{
		cleanup_duration: null,
		duration: ((date now) - $worker.start),
		execution_duration: null,
		name: $worker.name,
		output: {
			exit_code: 1,
			stderr: $message,
			stdout: '',
		},
		round: $worker.round,
		seq: $worker.seq,
		temp_path: '',
	}
}

def fskit_temp_root [] {
	$env.HOME | path join '.tangram/test-tmp'
}

# Create the pending entries for one round of tests. Each entry carries a unique sequence number, because in stress mode the same test may run concurrently with itself, so results cannot be matched to running entries by name.
def round_entries [tests: list, round: int, first_seq: int] {
	$tests | enumerate | each { |entry| $entry.item | merge { seq: ($first_seq + $entry.index), round: $round } }
}

def run_test [test: record, options: record] {
	# Create a temp directory for this test. With fskit, it must live under the
	# tangram directory, because that is the only path outside the app group
	# container the file system extension's sandbox permits.
	let temp_path = if $options.fskit {
		let root = fskit_temp_root
		mkdir $root
		mktemp -d --tmpdir-path $root 'tangram_test_XXXXXX' | path expand
	} else {
		mktemp -d -t tangram_test_XXXXXX | path expand
	}
	let working_path = $temp_path | path join 'work'
	mkdir $working_path

	# Remove inline, pending, and touch files. Skip this in stress mode, because concurrent runs of the same test would race on these files.
	let parsed = $test.path | path parse
	if not $options.stress {
		for path in (glob $'($parsed.parent | path join $parsed.stem){.{inline,pending,touched},/*.{pending,touched}}') {
			rm $path
		}
	}

	# Run the test.
	let start = date now
	let timeout = $options.timeout | into int | $in / 1_000_000_000
	mut config = {}
	if $options.preserve_temps {
		$config = $config | merge deep {
			advanced: {
				preserve_temp_directories: true,
			},
		}
	}
	if $options.vm {
		$config = $config | merge deep {
			sandbox: {
				isolation: {
					vm : {
						kernel_path: $options.kernel_path,
					},
				},
			},
		}
	}
	if not ($config | is-empty) {
		$config | to json | save -f ($temp_path | path join "config.json")
	}
	let fdb_cluster_path = if $options.cloud {
		let cluster_path = $env.TANGRAM_TEST_FDB_CLUSTER? | default ''
		if ($cluster_path | is-empty) {
			let cluster_path = $temp_path | path join 'fdb.cluster'
			foundationdb_cluster_description | save -f $cluster_path

			$cluster_path
		} else {
			$cluster_path
		}
	} else {
		''
	}
	let output = with-env {
		SHELL: "/bin/sh",
		TANGRAM_CONFIG: ($temp_path | path join "config.json"),
		TANGRAM_MODE: client,
		TANGRAM_QUIET: true,
		TANGRAM_TEST_CLOUD: (if $options.cloud { "1" } else { "" }),
		TANGRAM_TEST_DATABASE_POOL: $options.database_pool_path,
		TANGRAM_TEST_FDB_CLUSTER: $fdb_cluster_path,
		TANGRAM_TEST_FSKIT: (if $options.fskit { "1" } else { "" }),
		TANGRAM_TEST_KERNEL_PATH: $options.kernel_path,
		TANGRAM_TEST_OFFLINE: (if $options.offline { "1" } else { "" }),
		TANGRAM_TEST_QUICKJS: (if $options.quickjs { "1" } else { "" }),
		TANGRAM_TEST_TURSO: (if $options.turso { "1" } else { "" }),
		TANGRAM_TEST_VFS: (if $options.vfs { "1" } else { "" }),
		TANGRAM_TEST_VM: (if $options.vm { "1" } else { "" }),
		TMPDIR: $temp_path,
	} {
		let command = [
			$'use ($harness_path) cleanup_background_jobs'
			'$env.config.display_errors.exit_code = true;'
			$'cd ($working_path | to nuon);'
			'try {'
			$'source ($test.path);'
			'} finally {'
			$'cleanup_background_jobs ($temp_path);'
			'}'
		] | str join "\n"
		if $options.no_capture {
			open /dev/null | timeout --kill-after 5s $timeout bash -c (process_supervisor) _ $nu.pid nu -c $command o+e> /dev/stderr
			let exit_code = $env.LAST_EXIT_CODE
			{ exit_code: $exit_code, stdout: '', stderr: '' }
		} else {
			# Capture output in a file so a surviving process cannot hold a pipe open.
			let output_path = $temp_path | path join 'output'
			let exit_code = try {
				open /dev/null | timeout --kill-after 5s $timeout bash -c (process_supervisor) _ $nu.pid nu -c $command o+e> $output_path
				0
			} catch { |error|
				$error.exit_code? | default 1
			}
			let stderr = if ($output_path | path exists) {
				open --raw $output_path | decode utf-8
			} else {
				''
			}
			{ exit_code: $exit_code, stdout: '', stderr: $stderr }
		}
	}
	let execution_duration = (date now) - $start
	let cleanup_start = date now
	mut cleanup_errors = []

	# Terminate and report any process that escaped the inner test supervisor.
	let process_cleanup = try {
		{
			error: null,
			leaked_processes: (cleanup_test_processes $temp_path),
		}
	} catch { |error|
		{ error: $error, leaked_processes: [] }
	}
	if $process_cleanup.error != null {
		$cleanup_errors = $cleanup_errors | append $process_cleanup.error
	}
	let leaked_processes = $process_cleanup.leaked_processes
	if not ($leaked_processes | is-empty) {
		let pids = $leaked_processes | str join ', '
		$cleanup_errors = $cleanup_errors | append {
			msg: $'terminated test processes that survived cleanup: ($pids)'
		}
	}

	# If the test passed, delete snapshots which were not touched and remove touch files. Skip this in stress mode, because concurrent runs of the same test would race on these files.
	if $output.exit_code == 0 and not $options.stress {
		let parent_path = $test.path | path dirname
		let stem = $test.path | path parse | get stem
		for path in (glob $'($parent_path | path join $stem){.snapshot,/*.snapshot}') {
			if not ($path | str replace '.snapshot' '.touched' | path exists) {
				rm $path
			}
		}
		for path in (glob $'($parent_path | path join $stem){.touched,/*.touched}') {
			try { rm $path }
		}
	}

	# Clean up the cloud resources.
	let instances_path = $temp_path | path join 'instances'
	let instances = if ($instances_path | path exists) {
		open $instances_path | lines | where { $in != '' } | uniq
	} else {
		[]
	}
	let reset_results = $instances | par-each { |instance|
		let error = try {
			reset_database_instance $instance $options.database_pool_path

			null
		} catch { |error|
			$error
		}

		{ error: $error }
	}
	for result in $reset_results {
		if $result.error != null {
			$cleanup_errors = $cleanup_errors | append $result.error
		}
	}
	let output = if ($cleanup_errors | is-empty) {
		$output
	} else {
		let cleanup_stderr = $cleanup_errors | each { |error|
			let message = $error.msg? | default ($error | to nuon)
			let help = $error.help? | default '' | str trim
			let message = if ($help | is-empty) { $message } else { $"($message)\n($help)" }

			$'cleanup: ($message)'
		} | str join "\n"
		let stderr = [$output.stderr $cleanup_stderr] | where { not ($in | is-empty) } | str join "\n"
		let exit_code = if $output.exit_code in [0 77] { 1 } else { $output.exit_code }

		$output | upsert exit_code $exit_code | upsert stderr $stderr
	}

	# Clean up the temp directory.
	let preserve_temp = $options.preserve_temps or $output.exit_code not-in [0 77]
	let temp_cleanup_error = if $preserve_temp {
		null
	} else {
		try {
			remove_temp_directory $temp_path

			null
		} catch { |error|
			$error
		}
	}
	let output = if $temp_cleanup_error == null {
		$output
	} else {
		let message = $temp_cleanup_error.msg? | default ($temp_cleanup_error | to nuon)
		let stderr = [$output.stderr $'cleanup: ($message)'] | where { not ($in | is-empty) } | str join "\n"
		let exit_code = if $output.exit_code in [0 77] { 1 } else { $output.exit_code }

		$output | upsert exit_code $exit_code | upsert stderr $stderr
	}
	let cleanup_duration = (date now) - $cleanup_start
	let duration = (date now) - $start

	{
		cleanup_duration: $cleanup_duration,
		duration: $duration,
		execution_duration: $execution_duration,
		name: $test.name,
		output: $output,
		temp_path: $temp_path,
	}
}

def print_test_result [result: record, print_passing_test_output: bool] {
	let symbol = if $result.output.exit_code == 0 {
		$'(ansi green)✓(ansi reset)'
	} else if $result.output.exit_code == 77 {
		$'(ansi yellow)⊘(ansi reset)'
	} else {
		$'(ansi red)✗(ansi reset)'
	}
	let status = if (is_failed $result) {
		let exit_code = $result.output.exit_code
		let description = exit_description $exit_code

		$' — exit ($exit_code) (($description))'
	} else {
		''
	}
	print -e $'($symbol) ($result.name) ($result.duration)($status)'
	if $result.output.exit_code == 77 {
		let reason = $result.output.stderr | str trim
		if ($reason | str length) > 0 {
			print -e $'  ($reason)'
		}
	} else if $print_passing_test_output or $result.output.exit_code != 0 {
		if ($result.output.stderr | is-empty) {
			print -e '  no output was captured'
		} else {
			print -e -n $result.output.stderr
			if not ($result.output.stderr | str ends-with "\n") {
				print -e ''
			}
		}
		let execution_duration = $result.execution_duration?
		let cleanup_duration = $result.cleanup_duration?
		if $execution_duration != null or $cleanup_duration != null {
			print -e $'  phases: execution ($execution_duration | default "unknown"), cleanup ($cleanup_duration | default "unknown")'
		}
	}
}

def exit_description [exit_code: int] {
	if $exit_code == 124 {
		'timed out'
	} else if $exit_code == 137 {
		'killed with SIGKILL or after the timeout grace period'
	} else if $exit_code == 143 {
		'terminated with SIGTERM'
	} else if $exit_code > 128 {
		$'terminated by signal ($exit_code - 128)'
	} else {
		'failed'
	}
}

export def artifact [artifact] {
	def inner [artifact: any, path: string] {
		let artifact = if ($artifact | describe) == 'string' {
			{ kind: 'file', contents: (doc $artifact), executable: false }
		} else if (($artifact | describe) | str starts-with 'record') {
			if $artifact.kind? != null {
				$artifact
			} else {
				{ kind: 'directory', entries: $artifact }
			}
		} else {
			$artifact
		}
		match $artifact.kind {
			'directory' => {
				try { mkdir $path }
				for entry in ($artifact.entries | transpose name value) {
					inner $entry.value ($path | path join $entry.name)
				}
			}
			'file' => {
				$artifact.contents | save $path
				if $artifact.executable {
					chmod +x $path
				}
				for pair in (($artifact.xattrs? | default {}) | transpose key value) {
					xattr_write $pair.key $pair.value $path
				}
			}
			'symlink' => {
				ln -s $artifact.path $path
			}
		}
	}
	let path = mktemp -d | path join 'artifact'
	inner $artifact $path
	$path
}

export def directory [entries: record] {
	{ kind: 'directory', entries: $entries }
}

export def file [
	--executable (-x)
	--xattrs: record
	contents?: string
] {
	{ kind: 'file', contents: (doc ($contents | default '')), executable: $executable, xattrs: $xattrs }
}

export def symlink [path: string] {
	{ kind: 'symlink', path: $path }
}

export def doc [string: string] {
	# Split the lines.
	mut lines = $string | split row "\n"

	# Remove the first line if it is empty or contains only whitespace.
	if ($lines | length) > 0 and (($lines | first | str trim | str length) == 0) {
		$lines = $lines | skip 1
	}
	if ($lines | length) > 0 {
		let last = $lines | last
		if ($last | str trim | str length) == 0 {
			$lines = $lines | drop
		}
	}

	# Get the common leading whitespace prefix. Filter out lines that are empty or contain only whitespace.
	let non_whitespace_lines = $lines | where { |line|
		let trimmed = $line | str trim
		($trimmed | str length) > 0
	}

	# Extract leading whitespace from each non-empty line.
	let leading_whitespace = $non_whitespace_lines | each { |line|
		$line | split chars | take while { |char| $char == "\t" or $char == " " } | str join
	}

	# Find the common prefix of all leading whitespace strings.
	let common_prefix = if ($leading_whitespace | length) > 0 {
		$leading_whitespace | reduce { |it, acc|
			let acc_len = $acc | str length
			let it_len = $it | str length
			let min_len = if $acc_len < $it_len { $acc_len } else { $it_len }
			mut prefix_len = 0
			let acc_chars = $acc | split chars
			let it_chars = $it | split chars
			for i in 0..<$min_len {
				if ($acc_chars | get $i) == ($it_chars | get $i) {
					$prefix_len = $prefix_len + 1
				} else {
					break
				}
			}
			$acc | str substring ..<$prefix_len
		}
	} else {
		""
	}

	let prefix_len = $common_prefix | str length

	# Remove the common prefix from each line and combine them with newlines.
	let result = $lines
		| each { |line|
			if ($line | str length) >= $prefix_len {
				$line | str substring $prefix_len..
			} else {
				$line
			}
		}
		| str join "\n"

	$result
}

export def --env snapshot [
	--name: string
	--normalize (-n)
	--normalize-ids
	--path (-p)
	--redact (-r): oneof<list<string>, string>
	value: any
	inline?: string
] {
	let value = if $path {
		snapshot_path $value | to json -i 2
	} else {
		$value | to text
	}
	let redactions = if $redact == null {
		null
	} else if ($redact | describe) == 'string' {
		[$redact]
	} else {
		$redact
	}
	let value = if $redactions == null { $value } else { $value | redact ...$redactions }
	let value = if $normalize_ids {
		$value | normalize --normalize-ids
	} else if $normalize {
		$value | normalize
	} else {
		$value
	}

	if $inline != null {
		snapshot_inline --span=(metadata $inline).span $value $inline
	} else {
		snapshot_file --name=$name $value
	}
}

def --env snapshot_inline [
	--span: record
	value: string
	inline: string
] {
	# Get the expected value by processing the snapshot with doc.
	let expected_value = doc $inline

	# If the values match, return early.
	if $value == $expected_value {
		return
	}

	# Save the inline snapshot.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let inline_path = $test_directory_path | path join $'($test_name).inline'

	# Read existing inline data or start fresh.
	mut inline_entries = if ($inline_path | path exists) {
		open $inline_path | from json
	} else {
		[]
	}

	# Get the exact file position using view files.
	let files = view files
	let file = $files | where { |f| $span.start >= $f.start and $span.start < $f.end } | first
	let position = $span.start - $file.start
	let length = $span.end - $span.start

	# Add this entry.
	$inline_entries = $inline_entries | append {
		position: $position,
		length: $length,
		old: $expected_value,
		new: $value,
	}

	$inline_entries | to json | save -f $inline_path

	error make {
		msg: 'the snapshot does not match',
		help: (diff $expected_value $value),
		label: {
			span: $span,
			text: 'the snapshot',
		},
	}
}

def --env snapshot_file [
	--name (-n): string
	value: string
] {
	# Get the snapshot path.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let snapshot_directory_path = $test_directory_path | path join $test_name
	if $name != null {
		try { mkdir -v $snapshot_directory_path }
	}
	let snapshot_path = if $name == null {
		$test_directory_path | path join $'($test_name).snapshot'
	} else {
		$snapshot_directory_path | path join $'($name).snapshot'
	}
	let pending_path = $snapshot_path | str replace '.snapshot' '.pending'
	let touched_path = $snapshot_path | str replace '.snapshot' '.touched'

	# Touch the snapshot.
	touch $touched_path

	# Error if the snapshot does not exist.
	if not ($snapshot_path | path exists) {
		$value | save -f $pending_path
		error make {
			msg: 'the snapshot does not exist',
			label: {
				span: (metadata $value).span,
				text: 'the value',
			}
		}
	}

	# Read the snapshot.
	let old_value = open $snapshot_path

	# Error if the new value does not match the old value.
	if $value != $old_value {
		$value | save -f $pending_path
		error make {
			msg: 'the snapshot does not match',
			help: (diff $snapshot_path $pending_path --path),
			label: {
				span: (metadata $value).span,
				text: 'the value',
			},
		}
	}
}

def snapshot_path [path: string] {
	let $type = $path | path type
	if $type == 'dir' {
		let entries = ls -a $path
			| where name != ($path | path join '.') and name != ($path | path join '..')
			| each { |entry|
					let name = $entry.name | path basename
					let artifact = snapshot_path $entry.name
					{ name: $name, artifact: $artifact }
				}
			| reduce -f {} { |entry, acc|
					$acc | insert $entry.name $entry.artifact
				}
		{ kind: 'directory', entries: $entries }
	} else if $type == 'file' {
		let contents = open $path
		let executable = ls -l $path | first | get mode | str contains 'x'
		let names = xattr_list $path | where { |name| $name starts-with 'user.tangram' }
		let xattrs = $names | reduce -f {} { |name, acc| $acc | insert $name (xattr_read $name $path) }
		mut output = { kind: 'file', contents: $contents }
		if $executable {
			$output.executable = true
		}
		if not ($xattrs | is-empty) {
			$output.xattrs = $xattrs
		}
		$output
	} else if $type == 'symlink' {
		mut target = do -i { ls -l $path | first | get target }
		if $target == null {
			$target = (readlink $path | str trim)
		}
		{ kind: 'symlink', path: $target }
	}
}

export def --env "server spawn" [
	--busybox
	--cloud # Create a cloud-backed instance for the server.
	--config (-c): record
	--directory (-d): string
	--instance: record # Spawn the server in this instance.
	--name (-n): string
	--now: string # Set the server's simulated wall clock to an RFC 3339 timestamp.
	--preserve-keys
	--quickjs # Use QuickJS as the JS engine.
	--region: string # Set the server's region.
	--url (-u): string
] {
	let use_fskit = (($env.TANGRAM_TEST_FSKIT? | default "") | str length) > 0
	let server_config = $config | default {}
	let topology_keys = [instance primary_region region regions]
	let invalid_topology_keys = $server_config | columns | where { |key| $key in $topology_keys }
	if not ($invalid_topology_keys | is-empty) {
		error make {
			msg: $'server config contains topology fields: ($invalid_topology_keys | str join ", ")'
			help: 'set topology with instance and server spawn arguments'
		}
	}

	# Use unique semaphore names in the namespace FSKit can access.
	let store_posix_sem_prefix = if $use_fskit {
		let app_group_identifier = (identifiers).app_group_identifier
		$'($app_group_identifier)/((random chars) | str lowercase | str substring 0..5)'
	} else {
		$'/tg-((random chars) | str lowercase | str substring 0..7)'
	}

	mut default_config: any = {
		advanced: {
			disable_version_check: true
			internal_error_locations: false
		},
		index: {
			kind: 'lmdb',
			map_size: 10_485_760,
		},
		store: {
			kind: 'lmdb',
			map_size: 10_485_760,
			posix_sem_prefix: $store_posix_sem_prefix,
		},
		remotes: {},
		tokio_single_threaded: true,
		v8_thread_pool_size: 1,
	}

	let use_quickjs = $quickjs or (($env.TANGRAM_TEST_QUICKJS? | default "") | str length) > 0
	if $use_quickjs {
		$default_config = $default_config | merge deep {
			runner: {
				js: {
					engine: 'quickjs',
				},
			},
		}
	}

	let use_turso = (($env.TANGRAM_TEST_TURSO? | default "") | str length) > 0
	if $use_turso {
		$default_config = $default_config | merge deep {
			database: {
				kind: 'turso',
				path: 'database.sqlite3',
			},
		}
	}

	if $use_fskit {
		$default_config = $default_config | merge deep {
			vfs: {
				kind: 'fskit',
			},
		}
	}

	let use_vfs = (($env.TANGRAM_TEST_VFS? | default "") | str length) > 0

	let use_vm = (($env.TANGRAM_TEST_VM? | default "") | str length) > 0
	if $use_vm {
		let kernel_path = $env.TANGRAM_TEST_KERNEL_PATH? | default ""
		if ($kernel_path | str length) == 0 {
			error make { msg: 'TANGRAM_TEST_VM is set but TANGRAM_TEST_KERNEL_PATH is empty' }
		}
		$default_config = $default_config | merge deep {
			sandbox: {
				isolation: {
					vm: {
						kernel_path: $kernel_path,
					},
				},
			},
		}
	}

	if $cloud and $instance != null {
		error make { msg: '--cloud may not be combined with --instance' }
	}
	let instance = if $instance != null {
		$instance
	} else if $cloud {
		instance --cloud
	} else {
		instance
	}
	let instance_kind = $instance.kind
	let use_cloud = match $instance_kind {
		'cloud' => true,
		'local' => false,
		_ => {
			error make { msg: $'invalid instance kind: ($instance_kind)' }
		},
	}

	# Create the server directory. Local instances own one directory, while every cloud server owns its own directory.
	let directory_path = if $directory != null {
		$directory
	} else if $instance_kind == 'local' {
		$instance.directory
	} else {
		mktemp -d
	}
	try { mkdir $directory_path }

	let cloud_instance = if $use_cloud { $instance.id } else { null }
	let region_names = validate_instance_config $instance.config
	if not ($region_names | is-empty) and $region == null {
		error make { msg: 'a region is required when spawning a server in a regional instance' }
	}
	if $region != null and $region not-in $region_names {
		error make { msg: $'the server region is not in the instance regions list: ($region)' }
	}
	mut storage_instance: any = null
	if $use_cloud {
		if ($cloud_instance | is-empty) {
			error make { msg: 'the Tangram cloud instance is empty' }
		}
		let pool_path = $env.TANGRAM_TEST_DATABASE_POOL? | default ''
		if ($pool_path | is-empty) {
			error make { msg: 'TANGRAM_TEST_DATABASE_POOL is not set' }
		}
		let cluster = fdb_cluster
		$storage_instance = cloud_region_storage $cloud_instance $region $pool_path
		if ($storage_instance | is-empty) {
			error make { msg: 'the Tangram region storage instance is empty' }
		}
		track_database_pool_instance $storage_instance
		let partition_offset = $storage_instance | str replace 'pool' '' | into int

		let advanced = $default_config.advanced | merge {
			single_directory: false,
			single_process: false,
		}
		let config = {
			advanced: $advanced,
			database: {
				kind: 'postgres',
				read: {
					pool: {
						max: 1,
					},
					url: $'postgres://postgres@127.0.0.1:5432/database_($cloud_instance)?sslmode=disable',
				},
				write: {
					pool: {
						max: 1,
					},
					url: $'postgres://postgres@127.0.0.1:5432/database_($cloud_instance)?sslmode=disable',
				},
			},
			instance: $cloud_instance,
			index: {
				cluster: $cluster,
				instance: $storage_instance,
				kind: 'fdb',
			},
			messenger: {
				kind: 'nats',
				url: 'nats://127.0.0.1:4222',
			},
			store: {
				addr: '127.0.0.1:9042',
				connections: 1,
				keepalive: false,
				keyspace: $'store_($storage_instance)',
				kind: 'scylla',
				partition_offset: $partition_offset,
			},
			remotes: {},
		}
		$default_config = $default_config | merge $config
	}

	# Write the config.
	let config = $default_config
		| merge deep --strategy append $instance.config
		| merge deep --strategy append $server_config
	let config = if $instance.config.primary_region? == null {
		$config
	} else {
		$config | upsert primary_region $instance.config.primary_region
	}
	let config = if $instance.config.regions? == null {
		$config
	} else {
		$config | upsert regions $instance.config.regions
	}
	let config = if $use_cloud { $config | upsert instance $cloud_instance } else { $config }
	let config = if $region != null { $config | upsert region $region } else { $config }
	let config = if $use_cloud and $config.roles? == null {
		$config | upsert roles [cleaner http indexer scheduler]
	} else {
		$config
	}

	# Pin token keys to the server directory so restarts can verify existing tokens.
	let config = if $preserve_keys {
		let private_key_path = $directory_path | path join 'private_key'
		let public_key_path = $directory_path | path join 'public_key'
		if not ($private_key_path | path exists) {
			'U9ZBC697GDA0dlUBF/VVM4eqoJUVfQqwRNr6L2z8Ajg=' | decode base64 | save -f $private_key_path
			'MKmfiiYtaN4W/pP+V2hmmjtT2/+ILjYfiMJ9y4EsG1U=' | decode base64 | save -f $public_key_path
		}
		let keys = {
			private_key: {
				algorithm: 'ed25519',
				name: 'default',
				path: $private_key_path,
			},
			public_keys: [{
				algorithm: 'ed25519',
				name: 'default',
				path: $public_key_path,
			}],
		}
		$config | merge deep {
			authentication: {
				tokens: $keys,
			},
			authorization: {
				tokens: $keys,
			},
		}
	} else {
		$config
	}

	# Force the selected VFS unless the test disables it.
	let forced_vfs_kind = if $use_fskit { 'fskit' } else if $use_vfs { 'fuse' } else { null }
	let config = if $forced_vfs_kind == null {
		$config
	} else if ($config | get --optional vfs) == false {
		$config
	} else if (($config | get --optional vfs | describe) | str starts-with 'record') {
		$config | upsert vfs ($config | get vfs | upsert kind $forced_vfs_kind)
	} else {
		$config | upsert vfs { kind: $forced_vfs_kind }
	}
	let config_path = mktemp -d
	let config_path = $config_path | path join 'config.json'
	$config | to json | save -f $config_path

	# Determine the url.
	let url = $url | default (unix_socket_url $directory_path)
	$env.TANGRAM_URL = $url

	# Create a path for the server's captured output.
	let log_path = ($config_path | path dirname | path join 'log')
	touch $log_path
	let clock_path = if $now == null {
		null
	} else {
		let clock_path = $config_path | path dirname | path join 'clock'
		$now | save -f $clock_path
		$clock_path
	}

	# Create and start the server.
	let vfs = $config | get --optional vfs
	let checkout_directory_name = if $vfs == null or $vfs == false { 'store' } else { 'checkouts' }
	let checkout_directory = $directory_path | path join $checkout_directory_name
	let name = $name | default 'server'
	let server = {
		checkout_directory: $checkout_directory,
		clock: $clock_path,
		config: $config,
		config_path: $config_path,
		directory: $directory_path,
		exit: null,
		instance: $instance,
		job: null,
		log: $log_path,
		name: $name,
		url: $url,
	}
	let server = server start $server

	# Tag busybox if requested.
	if $busybox {
		skip_if_offline
		let path = mktemp -d
		let source = '
			const SOURCES: Record<string, { url: string, checksum: tg.Checksum }> = {
				"aarch64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_universal_darwin.tar.zst",
					checksum: "sha256:8e0031b8c5a183e173fe4b7c2d6b038c46b46f390f6ff5e1d23eb0ec403e2abe",
				},
				"x86_64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_universal_darwin.tar.zst",
					checksum: "sha256:8e0031b8c5a183e173fe4b7c2d6b038c46b46f390f6ff5e1d23eb0ec403e2abe",
				},
				"aarch64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_aarch64_linux.tar.zst",
					checksum: "sha256:b4724cfba44ea545fb041c61cdd86c0c8fdda1f221bfbe284c23853014faec6d",
				},
				"x86_64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/utils_x86_64_linux.tar.zst",
					checksum: "sha256:552e634483b6d118463bff342febc2b72665c48912e0bf90e80c897cf20b16a9",
				},
			};

			export const env = (host?: string) => {
				const host_ = host ?? tg.host.current;
				tg.assert(typeof host_ === "string");
				const kv = Object.entries(SOURCES).find(([k, _]) => k === host_);
				tg.assert(kv, `unknown host: ${host_}`);
				const { url, checksum } = kv[1];
				const dir = tg.download(url, checksum, { mode: "extract" }).then(tg.Directory.expect);
				return { PATH: tg.Mutation.suffix(tg`${dir}/bin`, ":") };
			};

			export default env;
		';
		$source | save ($path | path join 'tangram.ts')
		if ($config.authentication?.users?.providers?.insecure? | default false) {
			let user = tg -c ($config_path) login --verbose --name busyboxer | from json
			tg -c ($config_path) --token ($user.token) tag 'busybox' $path
			tg -c ($config_path) --token ($user.token) grant public tag_read 'busybox'
		} else {
			tg check $path
			tg -c ($config_path) tag 'busybox' $path
		}
		rm -rf $path
	}

	$server
}

export def --env "server start" [server: record] {
	let job_id = $server.job?
	if $job_id != null and not (job list | where id == $job_id | is-empty) {
		if (server_is_running $server) {
			error make { msg: 'the server is already running' }
		}
		try { job kill $job_id }
	}

	# Create the readiness and exit paths.
	let ready_path = $server.config_path | path dirname | path join 'ready'
	rm -f $ready_path
	touch $ready_path
	let server_exit_directory_path = (($env.TMPDIR? | default ($server.config_path | path dirname)) | path join $server_exit_directory_name)
	try { mkdir $server_exit_directory_path }

	# Create the environment.
	mut environment = {}
	if $server.clock? != null {
		$environment = $environment | upsert TANGRAM_TEST_CLOCK $server.clock
	}
	let macos_app_group_socket = create_macos_app_group_socket_path
	if $macos_app_group_socket != null {
		$environment = $environment | upsert TANGRAM_MACOS_APP_GROUP_SOCKET $macos_app_group_socket
	}
	$environment = $environment | upsert TANGRAM_TEST_READY_PATH $ready_path
	let environment = $environment

	# Start the server.
	let config_path = $server.config_path
	let directory = $server.directory
	let log_path = $server.log
	let name = $server.name
	let url = $server.url
	let server_job = job spawn -d server {
		let server_job_id = job id
		let exit_path = $server_exit_directory_path | path join $'($server_job_id).exit'
		do -i {
			with-env $environment {
				bash -c (process_supervisor) _ $nu.pid tangram -c $config_path -d $directory -u $url serve --ready-fd 3 e>| lines | each { |line|
					$"($line)\n" | save --append $log_path
					print -e $"($name): ($line)\r"
				}
			}
		}
		'' | save -f $exit_path
	}
	let exit_path = $server_exit_directory_path | path join $'($server_job).exit'

	# Wait for the server to be ready.
	let ready_timeout = 30sec
	let ready_timeout_secs = $ready_timeout | into int | $in / 1_000_000_000
	let ready_command = 'while [ ! -s "$1" ] && [ ! -e "$2" ]; do sleep 0.05; done; if [ -s "$1" ]; then od -An -t u1 -N1 "$1"; else exit 125; fi'
	let ready_output = (open /dev/null | timeout $ready_timeout_secs bash -c $ready_command _ $ready_path $exit_path | complete)
	rm -f $ready_path
	let ready_byte = $ready_output.stdout | str trim
	if $ready_output.exit_code != 0 {
		stop_server_job $server_job
		wait_for_server_exit $exit_path | ignore
		let message = if $ready_output.exit_code == 124 {
			$"the server did not signal readiness within ($ready_timeout)"
		} else {
			'the server exited before signaling readiness'
		}
		let log = if ($log_path | path exists) { open --raw $log_path | str trim } else { '' }
		if ($log | is-empty) {
			error make { msg: $message }
		} else {
			error make { msg: $message, help: $'server output:\n($log)' }
		}
	}
	if $ready_byte != '0' {
		stop_server_job $server_job
		wait_for_server_exit $exit_path | ignore
		let message = if ($ready_byte | is-empty) {
			'the server exited before signaling readiness; check the server output above'
		} else {
			$"the server signaled an invalid readiness byte: ($ready_byte)"
		}
		error make { msg: $message }
	}
	$env.TANGRAM_URL = $url
	let server = $server | upsert exit $exit_path | upsert job $server_job

	$server
}

export def "server stop" [server: record] {
	let job_id = $server.job?
	if $job_id == null or (job list | where id == $job_id | is-empty) {
		return
	}
	stop_test_server_processes $server.directory
	if not (wait_for_server_exit $server.exit) {
		stop_server_job $job_id
		if not (wait_for_server_exit $server.exit) {
			try { job kill $job_id }
			error make { msg: 'the server did not stop' }
		}
	}
}

export def --env "server restart" [server: record] {
	server stop $server
	let server = server start $server

	$server
}

def server_is_running [server: record] {
	let lock_path = $server.directory | path join 'lock'
	if not ($lock_path | path exists) {
		return false
	}
	let pid = try { open --raw $lock_path | str trim | into int } catch { null }
	if $pid == null {
		return false
	}

	not (ps | where pid == $pid | is-empty)
}

def create_macos_app_group_socket_path [] {
	if $nu.os-info.name != 'macos' {
		return null
	}
	let group_id = (identifiers).app_group_identifier
	let group_container = $env.HOME | path join 'Library/Group Containers' $group_id
	try { mkdir $group_container }
	let socket_name = $'socket-((random chars) | str lowercase)'

	$group_container | path join $socket_name
}

# Set a server's simulated wall clock.
export def set_time [server: record, now: string] {
	let clock_path = $server.clock?
	if $clock_path == null {
		error make { msg: 'the server does not have a test clock' }
	}
	let temporary_path = $clock_path + $'.((random chars) | str lowercase)'
	$now | save -f $temporary_path
	mv -f $temporary_path $clock_path
}

# Advance a server's simulated wall clock.
export def advance_time [server: record, duration: duration] {
	let clock_path = $server.clock?
	if $clock_path == null {
		error make { msg: 'the server does not have a test clock' }
	}
	let now = open --raw $clock_path | str trim | into datetime
	let now = ($now + $duration) | format date '%Y-%m-%dT%H:%M:%SZ'
	set_time $server $now
}

# Stop a server, so that its output is complete, and return the distinct errors
# it logged as '<target> <message>'. The server must have been spawned with
# `--config { tracing: { stderr_format: 'json' } }`.
export def server_errors [server: record] {
	server stop $server
	open --raw $server.log
		| lines
		| each { try { from json } catch { null } }
		| compact
		| where { |event| try { $event.level? == 'ERROR' } catch { false } }
		| each { |event| $"($event.target) ($event.fields.message)" }
		| uniq
		| sort
}

export def instance [
	--cloud
	--config: record
	--primary-region: string
	--regions: list
] {
	let config = $config | default {}
	let config = if $primary_region == null { $config } else { $config | upsert primary_region $primary_region }
	let config = if $regions == null { $config } else { $config | upsert regions $regions }
	let config = if $config.regions? == null {
		$config
	} else {
		$config | upsert regions (allocate_region_urls $config.regions)
	}
	validate_instance_config $config | ignore
	let use_cloud = $cloud and (($env.TANGRAM_TEST_CLOUD? | default '') | str length) > 0
	if not $use_cloud {
		let directory = mktemp -d

		return { config: $config, directory: $directory, kind: 'local' }
	}
	let pool_path = $env.TANGRAM_TEST_DATABASE_POOL? | default ''
	if ($pool_path | is-empty) {
		error make { msg: 'TANGRAM_TEST_DATABASE_POOL is not set' }
	}
	let id = acquire_database_instance $pool_path
	track_database_pool_instance $id

	{ config: $config, id: $id, kind: 'cloud' }
}

export def "instance region url" [instance: record, region: string] {
	let matches = $instance.config.regions? | default [] | where name == $region
	if ($matches | is-empty) {
		error make { msg: $'the instance does not contain region ($region)' }
	}

	$matches | first | get url
}

def allocate_region_urls [regions: list] {
	$regions | each { |region|
		if $region.url? != null {
			$region
		} else {
			let socket_directory = mktemp -d
			$region | upsert url (unix_socket_url $socket_directory)
		}
	}
}

def unix_socket_url [directory: string] {
	$'http+unix://($directory | url encode --all)%2Fsocket'
}

def validate_instance_config [config: record] {
	let invalid_topology_keys = $config | columns | where { |key| $key in [instance region] }
	if not ($invalid_topology_keys | is-empty) {
		error make {
			msg: $'instance config contains server topology fields: ($invalid_topology_keys | str join ", ")'
			help: 'set the server region with --region'
		}
	}
	let primary_region = $config.primary_region?
	let regions = $config.regions?
	if $primary_region != null and $regions == null {
		error make { msg: 'regions are required when a primary region is set' }
	}
	let region_names = $regions | default [] | get name
	if ($region_names | uniq | length) != ($region_names | length) {
		error make { msg: 'the instance regions list contains duplicate names' }
	}
	if $primary_region != null and $primary_region not-in $region_names {
		error make { msg: $'the primary region is not in the regions list: ($primary_region)' }
	}

	$region_names
}

def track_database_pool_instance [instance: string] {
	$instance ++ "\n" | save --append (($nu.temp-dir? | default $nu.temp-path?) | path join 'instances')
}

def cloud_region_storage [instance: string, region: any, pool_path: string] {
	let region = $region | default '' | into string
	let key = $'($instance)\n($region)' | hash sha256
	let storages_path = ($nu.temp-dir? | default $nu.temp-path?) | path join 'region_storages'
	mkdir $storages_path
	let storage_path = $storages_path | path join $key
	if ($storage_path | path exists) {
		return (open --raw $storage_path | str trim)
	}
	let lock_path = $storage_path + '.lock'
	let lock_output = ^mkdir $lock_path | complete
	if $lock_output.exit_code != 0 {
		let output = (open /dev/null | timeout 10 bash -c 'while [ ! -s "$1" ]; do sleep 0.05; done' _ $storage_path | complete)
		if $output.exit_code != 0 {
			error make { msg: $'timed out waiting for the Tangram region storage: ($region)' }
		}

		return (open --raw $storage_path | str trim)
	}
	let storage = if (try_claim_cloud_instance_storage $instance) {
		$instance
	} else {
		acquire_database_instance $pool_path
	}
	$storage | save -f $storage_path
	rm $lock_path

	$storage
}

def try_claim_cloud_instance_storage [instance: string] {
	let claims_path = ($nu.temp-dir? | default $nu.temp-path?) | path join 'instance_storage_claims'
	mkdir $claims_path
	let claim_path = $claims_path | path join $instance
	let output = ^mkdir $claim_path | complete

	$output.exit_code == 0
}

def reset_database_instance [instance: string, pool_path: string] {
	mut reset_errors = []
	for attempt in 1..$database_reset_attempts {
		let reset_error = try {
			reset_database_instance_once $instance $pool_path

			null
		} catch { |error|
			$error
		}
		if $reset_error == null {
			return
		}
		$reset_errors = $reset_errors | append $reset_error
		if $attempt < $database_reset_attempts {
			sleep 250ms
		}
	}
	let last_error = $reset_errors | last
	let message = $last_error.msg? | default ($last_error | to nuon)
	error make { msg: $'failed to reset database pool instance ($instance) after ($database_reset_attempts) attempts: ($message)' }
}

def reset_database_instance_once [instance: string, pool_path: string] {
	let database_reset_timeout_secs = $database_reset_timeout | into int | $in / 1_000_000_000
	let foundationdb_reset_timeout_secs = $database_reset_timeout_secs + 3
	let postgres_schema_path = $repository_path | path join packages/server/src/database/postgres.sql
	let postgres_tables = open --raw $postgres_schema_path
		| lines
		| parse --regex '^create table (?<table>[a-z_]+) \('
		| get table
	let postgres_query = $postgres_tables
		| reverse
		| each { |table| $'delete from ($table);' }
		| prepend 'begin;'
		| append 'insert into index_outbox_batch (next) values (0);'
		| append 'commit;'
		| str join "\n"
	let foundationdb_command = foundationdb_command $database_reset_timeout
	let results = ['foundationdb' 'postgres' 'scylla'] | par-each { |database|
		let output = match $database {
			'foundationdb' => {
				(^timeout --kill-after 2s $foundationdb_reset_timeout_secs ...$foundationdb_command --exec $'writemode on; clearrange "($instance)" "($instance)\xff"' | complete)
			},
			'postgres' => {
				(^timeout --kill-after 2s $database_reset_timeout_secs psql --host=127.0.0.1 --username=postgres --dbname=$'database_($instance)' --set=ON_ERROR_STOP=1 --command $postgres_query | complete)
			},
			'scylla' => {
				(^timeout --kill-after 2s $database_reset_timeout_secs tangram_scylla_client 127.0.0.1 9042 -k $'store_($instance)' -e 'truncate logs; truncate objects; truncate object_cache; truncate object_index_outbox; truncate object_archive_outbox;' | complete)
			},
		}

		{ database: $database, output: $output }
	}
	let failures = $results | where { |result| $result.output.exit_code != 0 }
	if not ($failures | is-empty) {
		let details = $failures | each { |failure|
			let message = [$failure.output.stderr? $failure.output.stdout?]
				| each { str trim }
				| where { not ($in | is-empty) }
				| str join "\n"
			let message = if ($message | is-empty) { $'exit code ($failure.output.exit_code)' } else { $message }

			$'($failure.database): ($message)'
		} | str join "\n"
		error make {
			msg: $'failed to reset database pool instance ($instance):\n($details)'
		}
	}

	let lease_path = $pool_path | path join $instance lease
	if not ($lease_path | path exists) {
		error make { msg: $'the lease for database pool instance ($instance) does not exist' }
	}
	^rm -f $lease_path
}

def diff [old: string, new: string, --path] {
	let old_path = if $path { $old } else { let t = mktemp; $old | save -f $t; $t }
	let new_path = if $path { $new } else { let t = mktemp; $new | save -f $t; $t }
	let result = delta --file-style=omit --hunk-header-style=omit --no-gitconfig $old_path $new_path | complete | get stdout
	if not $path { rm $old_path $new_path }
	$result
}

def literal [value: string, indent: string] {
	let raw = $value | str contains "'"
	let open = if $raw { "r#'" } else { "'" }
	let close = if $raw { "'#" } else { "'" }
	if ($value | str contains "\n") {
		let has_trailing_newline = $value | str ends-with "\n"
		let trimmed = $value | str trim --right --char "\n"
		let indented = $trimmed | split row "\n" | each { |line| $"($indent)\t($line)" } | str join "\n"
		if $has_trailing_newline {
			$"($open)\n($indented)\n\n($indent)($close)"
		} else {
			$"($open)\n($indented)\n($indent)($close)"
		}
	} else {
		$"($open)($value)($close)"
	}
}

def get_indent [source: string, position: int] {
	let before = $source | str substring ..<$position
	let line_start = $before | str index-of "\n" --end
	let line_start = if $line_start == -1 { 0 } else { $line_start + 1 }
	let line_prefix = $source | str substring $line_start..<$position
	$line_prefix | parse --regex '^(\s*)' | get 0.capture0? | default ''
}

export def --env success [
	output: record
	message?: string
] {
	if $output.exit_code != 0 {
		error make {
			msg: ($message | default 'the process failed'),
			label: {
				span: (metadata $output).span,
				text: 'the output',
			},
			help: $output.stderr,
		}
	}
}

# Skip the test, reporting the reason. The runner treats exit code 77 as skipped rather than passed or failed. This is named skip_test because skip is a Nushell builtin.
export def skip_test [reason: string] {
	print --stderr $reason
	exit 77
}

# Skip the test when the runner was invoked with --offline. Call this at the top of tests which require network access.
export def skip_if_offline [] {
	if (($env.TANGRAM_TEST_OFFLINE? | default '') | str length) > 0 {
		skip_test 'this test requires network access'
	}
}

export def skip_if_no_cloud [] {
	let cloud = (($env.TANGRAM_TEST_CLOUD? | default '') | str length) > 0
	if not $cloud {
		skip_test 'this test requires cloud databases'
	}
}

# Determine whether the running kernel has enabled the FUSE io_uring transport.
export def fuse_io_uring_available [] {
	if $nu.os-info.name != 'linux' {
		return false
	}
	let path = '/sys/module/fuse/parameters/enable_uring'
	if not ($path | path exists) {
		return false
	}
	try {
		(open --raw $path | str trim | str lowercase) in ['1' 'y']
	} catch {
		false
	}
}

# Poll a condition until it returns true, erroring if the timeout elapses. Prefer this over a bare sleep, so the test runs as soon as the condition holds and tolerates slow machines.
export def wait_until [
	condition: closure
	message?: string
	--timeout: duration = 10sec
	--interval: duration = 50ms
] {
	let start = date now
	loop {
		if (do $condition) {
			return
		}
		if ((date now) - $start) > $timeout {
			error make {
				msg: ($message | default 'the condition was not met within the timeout'),
				label: {
					span: (metadata $condition).span,
					text: 'the condition',
				},
			}
		}
		sleep $interval
	}
}

# Redact literal strings in the input for snapshotting.
export def redact [...redactions: string] {
	mut output = $in
	for redaction in ($redactions | sort-by { |redaction| $redaction | str length } --reverse) {
		$output = $output | str replace --all $redaction '<redacted>'
	}
	$output
}

export def --env failure [
	output: record
	message?: string
] {
	if $output.exit_code == 0 {
		error make {
			msg: ($message | default 'the process succeeded'),
			label: {
				span: (metadata $output).span,
				text: 'the output',
			},
			help: $output.stderr,
		}
	}
}

export def xattr_list [path: string] {
	match $nu.os-info.name {
		'macos' => { xattr $path | lines }
		'linux' => { getfattr -m '.' $path | complete | get stdout | lines | where { |l| not ($l starts-with '#') and $l != '' } }
	}
}

export def xattr_read [name: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -p $name $path | str trim }
		'linux' => { getfattr -n $name --only-values $path | str trim }
	}
}

export def xattr_write [name: string, value: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -w $name $value $path }
		'linux' => { setfattr -n $name -v $value $path }
	}
}

# Normalize runtime IDs and tokens in a string for snapshotting. With --normalize-ids, include content-addressed IDs. The length floor keeps identifiers such as `pcs_id` from being normalized.
export def normalize [value?: string, --normalize-ids] {
	let input = $in
	let value = ($value | default $input)

	mut output = $value
	mut numeric_process_index = 0
	for id in ($output | parse --regex 'id = (?<id>[0-9]+)' | get id | uniq) {
		if $numeric_process_index > 9 {
			error make { msg: 'too many IDs to normalize for the prefix' }
		}
		let digit = $numeric_process_index | into string
		let replacement = 'pcs_00' + (0..<26 | each { $digit } | str join)
		$numeric_process_index += 1
		$output = $output | str replace --all $'id = ($id)' $'id = ($replacement)'
	}

	let prefixes = if $normalize_ids {
		[blb cmd dir err fil gph grp org pcs sbx sym tag usr]
	} else {
		[err grp org pcs sbx tag usr]
	}
	let prefixes_pattern = $prefixes | str join '|'
	let pattern = '(?<id>(' + $prefixes_pattern + ')_[a-z0-9]{20,})'
	mut counters = {}
	for id in ($output | parse --regex $pattern | get id | uniq) {
		let prefix = ($id | split row '_' | first)
		let suffix = ($id | split row '_' | last)
		let index = ($counters | get --optional $prefix | default 0)
		if $index > 9 {
			error make { msg: 'too many IDs to normalize for the prefix' }
		}
		let header_length = if ($suffix | str length) < 2 { $suffix | str length } else { 2 }
		let header = ($suffix | str substring 0..<$header_length)
		let digit = ($index | into string)
		let replacement_length = ($suffix | str length) - $header_length
		let replacement_suffix = if $replacement_length <= 0 {
			''
		} else {
			0..<$replacement_length | each { $digit } | str join
		}
		let replacement = $'($prefix)_($header)($replacement_suffix)'
		$counters = ($counters | upsert $prefix ($index + 1))
		$output = $output | str replace --all $id $replacement
	}

	$output = $output | str replace --all --regex '([?&]token=|"token":\s*")[A-Za-z0-9._~%+/=-]+' '${1}<token>'
	$output = $output | str replace --all --regex '0\.[A-Za-z0-9_~%+/=-]+\.[A-Za-z0-9_~%+/=-]+\.[A-Za-z0-9_~%+/=-]+' '<token>'

	$output
}

def server_exit_path [temp_path: string, job_id: int] {
	$temp_path | path join $server_exit_directory_name | path join $'($job_id).exit'
}

def count_tangram_processes [] {
	tangram_process_pids_list | length
}

def tangram_process_pids_list [] {
	let output = (^bash -c (tangram_process_pids) | complete)
	if $output.exit_code != 0 {
		return []
	}
	$output.stdout | lines | str trim | where { not ($in | is-empty) }
}

def lmdb_sysv_keys_for_test_dirs [paths: list] {
	let lockfiles = $paths | each { |path|
		[
			(glob ($path | path join '**/index-lock')),
			(glob ($path | path join '**/logs-lock')),
			(glob ($path | path join '**/objects-lock')),
		] | flatten
	} | flatten | uniq
	if ($lockfiles | is-empty) {
		return []
	}
	let output = (
		^/usr/bin/perl -MIPC::SysV=ftok -e 'for my $path (@ARGV) { my $key = ftok($path, ord("M")); printf "0x%08x\n", $key if defined($key) && $key != -1; }' ...$lockfiles | complete
	)
	if $output.exit_code != 0 {
		return []
	}
	$output.stdout | lines | where { not ($in | is-empty) } | uniq
}

def count_lmdb_sysv_semaphores [keys: list] {
	if ($keys | is-empty) {
		return 0
	}
	let output = (^ipcs -s | complete)
	if $output.exit_code != 0 {
		return 0
	}
	let user = $env.USER? | default ''
	$output.stdout | lines | skip 3 | where { |line|
		let columns = $line | split row --regex '\s+' | where { $in != '' }
		($columns | length) >= 5 and ($columns | get 2) in $keys and (($user | is-empty) or (($columns | get 4) == $user))
	} | length
}

def process_supervisor [] {
	r#'
	set -u
	parent_pid=$1
	shift
	if [ -n "${TANGRAM_TEST_READY_PATH:-}" ]; then
		exec 3>"$TANGRAM_TEST_READY_PATH"
		unset TANGRAM_TEST_READY_PATH
	fi

	containment=group
	if command -v setsid >/dev/null 2>&1 && ps -o sid= -p $$ >/dev/null 2>&1; then
		setsid "$@" &
		child=$!
		containment=session
	else
		set -m
		"$@" &
		child=$!
		set +m
	fi

	child_done() {
		stat=$(ps -o stat= -p "$child" 2>/dev/null | tr -d " ")
		case "$stat" in
			""|Z*) return 0 ;;
			*) return 1 ;;
		esac
	}

	tree_done() {
		if [ "$containment" = session ]; then
			field=sid
		else
			field=pgid
		fi
		! ps -axo "$field=,stat=" | awk -v id="$child" '$1 == id && $2 !~ /^Z/ { found = 1 } END { exit found ? 0 : 1 }'
	}

	signal_tree() {
		signal=$1
		if [ "$containment" = session ]; then
			pkill -"$signal" -s "$child" 2>/dev/null || true
		else
			kill -"$signal" -- -"$child" 2>/dev/null || true
		fi
		kill -"$signal" "$child" 2>/dev/null || true
	}

	terminate_tree() {
		signal_tree TERM
		for _ in $(seq 1 60); do
			if child_done && tree_done; then
				return
			fi
			sleep 0.05
		done
		signal_tree KILL
	}

	(
		while kill -0 "$parent_pid" 2>/dev/null && ! child_done; do
			sleep 0.05
		done
		if ! child_done; then
			terminate_tree
		fi
	) &
	watcher=$!

	trap "terminate_tree" TERM INT HUP

	wait "$child" 2>/dev/null
	status=$?
	if ! tree_done; then
		terminate_tree
	fi

	trap - TERM INT HUP
	kill "$watcher" 2>/dev/null || true
	wait "$watcher" 2>/dev/null || true
	exit "$status"
	'#
}

def cleanup_test_processes [temp_path: string] {
	let output = (^bash -c (test_process_cleanup_script) _ $temp_path | complete)
	if $output.exit_code != 0 {
		let details = [$output.stderr $output.stdout]
			| each { str trim }
			| where { not ($in | is-empty) }
			| str join "\n"
		error make {
			msg: $'failed to clean up processes for the test temp directory: ($temp_path)'
			help: $details
		}
	}

	$output.stdout | lines | str trim | where { not ($in | is-empty) } | uniq
}

def test_process_cleanup_script [] {
	r#'
	set -u
	path=$1
	current_pgid=$(ps -o pgid= -p $$ | tr -d ' ')

	list_targets() {
		ps -axo pid=,pgid=,stat=,command= | while read -r pid pgid stat command; do
			case "$stat" in
				Z*) continue ;;
			esac
			if [ "$pgid" = "$current_pgid" ]; then
				continue
			fi
			case "$command" in
				*"$path"*) printf '%s %s\n' "$pid" "$pgid" ;;
			esac
		done
	}

	describe_targets() {
		while read -r pid _; do
			command=$(ps -o command= -p "$pid" 2>/dev/null || true)
			printf '%s (%s)\n' "$pid" "$command"
		done
	}

	targets=$(list_targets)
	if [ -z "$targets" ]; then
		exit 0
	fi

	# Allow descendants time to finish asynchronous teardown before treating them as leaks.
	for _ in $(seq 1 40); do
		sleep 0.05
		targets=$(list_targets)
		if [ -z "$targets" ]; then
			exit 0
		fi
	done
	descriptions=$(printf '%s\n' "$targets" | describe_targets)

	printf '%s\n' "$targets" | while read -r pid pgid; do
		kill -TERM -- -"$pgid" 2>/dev/null || true
		kill -TERM "$pid" 2>/dev/null || true
	done

	for _ in $(seq 1 60); do
		remaining=$(list_targets)
		if [ -z "$remaining" ]; then
			printf '%s\n' "$descriptions"
			exit 0
		fi
		sleep 0.05
	done

	remaining=$(list_targets)
	printf '%s\n' "$remaining" | while read -r pid pgid; do
		kill -KILL -- -"$pgid" 2>/dev/null || true
		kill -KILL "$pid" 2>/dev/null || true
	done

	for _ in $(seq 1 20); do
		remaining=$(list_targets)
		if [ -z "$remaining" ]; then
			printf '%s\n' "$descriptions"
			exit 0
		fi
		sleep 0.05
	done

	echo 'the following processes survived SIGKILL:' >&2
	list_targets >&2
	exit 1
	'#
}

def fdb_cluster [] {
	let env_cluster = $env.TANGRAM_TEST_FDB_CLUSTER? | default ''
	if ($env_cluster | str length) > 0 {
		return $env_cluster
	}

	let cluster = mktemp -t
	foundationdb_cluster_description | save -f $cluster
	$cluster
}

def clean_tangram_processes [] {
	let pids = tangram_process_pids_list
	if ($pids | is-empty) {
		return
	}
	for pid in $pids {
		try { ^bash -c 'kill -TERM -- -"$1" 2>/dev/null || true; kill -TERM "$1" 2>/dev/null || true' _ $pid }
	}
	for _ in 1..100 {
		let remaining = $pids | where { |pid|
			(^bash -c 'kill -0 "$1" 2>/dev/null' _ $pid | complete).exit_code == 0
		}
		if ($remaining | is-empty) {
			return
		}
		sleep 50ms
	}
	for pid in $pids {
		try { ^bash -c 'kill -KILL -- -"$1" 2>/dev/null || true; kill -KILL "$1" 2>/dev/null || true' _ $pid }
	}
}

def tangram_process_pids [] {
	'
		ps -axo pid=,command= | while read -r pid command; do
			if [ -z "$pid" ] || [ -z "$command" ]; then
				continue
			fi
			executable=${command%% *}
			case "$(basename "$executable" 2>/dev/null)" in
				tangram|tg) ;;
				*) continue ;;
			esac
			path=$(realpath "$executable" 2>/dev/null || true)
			if [ "$(basename "$path" 2>/dev/null)" = "tangram" ]; then
				printf "%s\n" "$pid"
			fi
		done
	'
}

def clean_lmdb_sysv_semaphores [keys: list] {
	if ($keys | is-empty) {
		return
	}
	let user = $env.USER? | default ''
	let output = (^ipcs -s | complete)
	if $output.exit_code != 0 {
		return
	}
	let semaphore_ids = $output.stdout | lines | skip 3 | where { |line|
		let columns = $line | split row --regex '\s+' | where { $in != '' }
		($columns | length) >= 5 and ($columns | get 2) in $keys and (($user | is-empty) or (($columns | get 4) == $user))
	} | each { |line|
		$line | split row --regex '\s+' | where { $in != '' } | get 1
	}
	for id in $semaphore_ids {
		try { ^ipcrm -s $id }
	}
}

export def cleanup_background_jobs [temp_path: string] {
	# Kill any background jobs started by the test, such as server and LSP processes.
	for job in (job list | where { ($in.description? | default '') == 'lsp' }) {
		for pid in ($job.pids? | default []) {
			try { ^bash -c 'kill -KILL -- -"$1" 2>/dev/null || true; kill -KILL "$1" 2>/dev/null || true' _ $pid }
		}
		try { job kill $job.id }
	}

	# Signal the actual servers before their Nu job wrappers so they can tear down their sandboxes and mounts.
	stop_test_server_processes $temp_path
	for job in (job list | where { ($in.description? | default '') == 'server' } | sort-by id | reverse) {
		let exit_path = server_exit_path $temp_path $job.id
		if not (wait_for_server_exit $exit_path) {
			stop_server_job $job.id
			if not (wait_for_server_exit $exit_path) {
				try { job kill $job.id }
			}
		}
	}

	# Kill every remaining test job, including unlabelled helpers and mock services.
	for job in (job list) {
		for pid in ($job.pids? | default []) {
			try { ^bash -c 'kill -TERM -- -"$1" 2>/dev/null || true; kill -TERM "$1" 2>/dev/null || true' _ $pid }
		}
		try { job kill $job.id }
	}
}

def stop_test_server_processes [path: string] {
	let lock_paths = try { glob ($path | path join '**/lock') } catch { [] }
	for lock_path in $lock_paths {
		let pid = try { open --raw $lock_path | str trim | into int } catch { null }
		if $pid == null {
			continue
		}
		let output = (^ps -o command= -p $pid | complete)
		if $output.exit_code == 0 and ($output.stdout | str contains $path) and ($output.stdout | str contains ' serve') {
			try { kill --quiet $pid }
		}
	}
}

def stop_server_job [job_id: int] {
	for job in (job list | where id == $job_id) {
		for pid in ($job.pids? | default []) {
			try { ^bash -c 'children=$(pgrep -P "$1" 2>/dev/null || true); kill -TERM "$1" 2>/dev/null || true; for child in $children; do command=$(ps -o command= -p "$child" 2>/dev/null || true); case "$command" in *" sandbox "*) ;; *) kill -TERM "$child" 2>/dev/null || true ;; esac; done' _ $pid }
		}
	}
}

def wait_for_server_exit [path: string] {
	if ($path | path exists) {
		return true
	}
	let output = (open /dev/null | timeout 10 bash -c 'while [ ! -e "$1" ]; do sleep 0.05; done' _ $path | complete)
	$output.exit_code == 0 or ($path | path exists)
}

def remove_temp_directory [path: string] {
	if not ($path | path exists) {
		return
	}
	force_unmount_vfs $path
	let chmod_output = (^timeout --kill-after 2s 10 chmod -R u+rwx $path | complete)
	let remove_output = (^timeout --kill-after 2s 10 rm -rf -- $path | complete)
	if $remove_output.exit_code != 0 {
		let details = [$chmod_output.stderr $remove_output.stderr]
			| each { str trim }
			| where { not ($in | is-empty) }
			| str join "\n"
		error make {
			msg: $'failed to remove the test temp directory: ($path)'
			help: $details
		}
	}
}

def force_unmount_vfs [path: string] {
	match $nu.os-info.name {
		'linux' => { force_unmount_vfs_linux $path },
		'macos' => { force_unmount_vfs_macos $path },
		_ => {},
	}
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
		try { ^timeout --kill-after 2s 5 umount -f $store_path o> /dev/null e> /dev/null }
	}
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

def fskit_provider_pids [executable: path] {
	ps --long
	| where { |process|
		($process.command? | default '') | str starts-with $executable
	}
	| get pid
}

def force_unmount_vfs_linux [path: string] {
	let mounted_store_paths = (
		try {
				^timeout 5 findmnt -rn -o TARGET | lines | where { |target|
				($target == ($path | path join 'store')) or (($target | str starts-with ($path + '/')) and (($target | path basename) == 'store'))
			}
		} catch {
			[]
		}
	)
	let store_paths = (
		[
			($path | path join 'store')
		] | append (
			try {
				^timeout 5 fd -a -t d '^store$' $path | lines
			} catch {
				[]
			}
		) | append $mounted_store_paths | uniq | each { |path|
			{ path: $path, length: ($path | str length) }
		} | sort-by length --reverse | get path
	)
	for store_path in $store_paths {
		try {
			^timeout --kill-after 2s 5 fusermount3 -u -z $store_path o> /dev/null e> /dev/null
		}
	}
}
