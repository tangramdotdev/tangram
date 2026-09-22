#!/usr/bin/env nu

use std/util 'path add'
use ../../scripts/macos/identifiers.nu

use tests/lib/test.nu *

const repository_path = path self '../../'
const test_library_path = path self 'tests/lib/test.nu'
const database_pool_directory_name = 'tangram_test_database_pool'
const database_reset_attempts = 2
const database_reset_timeout = 30sec
const foundationdb_container_name = 'tangram_test_foundationdb'
const foundationdb_image = 'foundationdb/foundationdb:7.3.68'
const scylla_container_name = 'tangram_test_scylla'
const scylla_image = 'scylladb/scylla:2026.3.0'
const worker_cleanup_grace = 75sec
const worker_exit_grace = 2sec

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
			remove_temp_directory --force-vfs-cleanup $path
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
			let keyspaces = $scylla_output.stdout | lines | str trim | where { $in starts-with '{' } | each { $in | from json | get keyspace_name } | where { $in starts-with 'cache_' }
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

	# Select the tests before building their dependencies.
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

	# Tests that require the Node.js client must live under node/.
	let node = $tests | any { |test| $test.name | str starts-with 'node/' }
	let extension_args = if $node { ['--package' 'tangram_client_native'] } else { [] }

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
			cargo build --release --all-features --package tangram_cli --package tangram_scylla_client ...$extension_args
		} else {
			cargo build --release --all-features --package tangram_cli ...$extension_args
		}
		ln -sf tangram target/release/tg
		path add ($repository_path | path join 'target/release')
	} else {
		if $cloud {
			cargo build --all-features --package tangram_cli --package tangram_scylla_client ...$extension_args
		} else {
			cargo build --all-features --package tangram_cli ...$extension_args
		}
		ln -sf tangram target/debug/tg
		path add ($repository_path | path join 'target/debug')
	}

	# Build the Node.js client.
	if $node {
		let client_profile = if $release { 'release' } else { 'dev' }
		let client_build_args = if $tangram_path == null { ['--skip-extension-build'] } else { [] }
		bun run --filter @tangramdotdev/client build --profile $client_profile ...$client_build_args
	}

	if $cloud {
		check_databases
		reclaim_stale_database_leases (database_pool_path)
	}
	let database_pool_path = if $cloud { database_pool_path } else { '' }

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
		preserve_failing_temps: $preserve_failing_temps,
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

	let preserved_results = $results | where { |result|
		$preserve_temps or ($preserve_failing_temps and (is_failed $result))
	} | where { |result| not ($result.temp_path | is-empty) }
	if not ($preserved_results | is-empty) {
		print -e ''
		print -e 'preserved temp directories:'
		for result in $preserved_results {
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
	let scylla_schema_path = $repository_path | path join packages/cache/src/scylla.cql
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
	if $options.preserve_failing_temps or $options.preserve_temps {
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
			$'use ($test_library_path) cleanup_background_jobs'
			'$env.config.display_errors.exit_code = true;'
			$'cd ($working_path | to nuon);'
			'try {'
			$'source ($test.path);'
			'} finally {'
			$'cleanup_background_jobs ($temp_path);'
			'}'
		] | str join "\n"
		if $options.no_capture {
			open /dev/null | timeout --kill-after 5s $timeout bash -c (process_supervisor) _ $nu.pid /dev/null nu -c $command o+e> /dev/stderr
			let exit_code = $env.LAST_EXIT_CODE
			{ exit_code: $exit_code, stdout: '', stderr: '' }
		} else {
			# Capture output in a file so a surviving process cannot hold a pipe open.
			let output_path = $temp_path | path join 'output'
			let exit_code = try {
				open /dev/null | timeout --kill-after 5s $timeout bash -c (process_supervisor) _ $nu.pid /dev/null nu -c $command o+e> $output_path
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
	let preserve_temp = $options.preserve_temps or ($options.preserve_failing_temps and $output.exit_code not-in [0 77])
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
		| append 'insert into index_queue_batch (next) values (0);'
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
				(^timeout --kill-after 2s $database_reset_timeout_secs tangram_scylla_client 127.0.0.1 9042 -k $'cache_($instance)' -e 'truncate archive_queue; truncate index_queue; truncate logs; truncate object_cache; truncate objects;' | complete)
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

def remove_temp_directory [--force-vfs-cleanup, path: string] {
	if not ($path | path exists) {
		return
	}
	let marker_path = $path | path join $vfs_cleanup_marker_name
	if $force_vfs_cleanup or ($marker_path | path exists) {
		force_unmount_vfs $path
	}
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
