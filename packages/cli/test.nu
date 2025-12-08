#!/usr/bin/env nu

use std/util 'path add'

export use std assert

const repository_path = path self '../../'

def main [
	--jobs (-j): int # The number of concurrent tests to run.
	--no-capture # Do not capture the output of each test. This sets --jobs to 1.
	--print-passing-test-output # Print the output of passing tests.
	--review (-r) # Review snapshots.
	--timeout: duration = 10sec # The timeout for each test.
	filter: string = '.*' # Filter tests.
] {
	# Add the debug build to the path.
	cargo build --features=nats,postgres,scylla
	ln -sf tangram target/debug/tg
	path add ($repository_path | path join 'target/debug')

	# Get the matching tests.
	let tests_path = ($repository_path | path join 'packages/cli/tests')
	let tests = fd -e nu -p $filter $tests_path | lines | each { |path|
		{
			path: $path,
			name: ($path | path relative-to $tests_path)
		}
	}

	# Create the state.
	mut pending = $tests
	mut running = []
	mut results = []

	let start = date now
	let total = $pending | length

	# Determine the number of concurrent tests to run.
	let jobs = $jobs | default (sys cpu | length)
	let jobs = if $no_capture {
		1
	} else {
		$jobs
	}

	def spawn [test: record] {
		job spawn {
			# Create a temp directory for this test.
			let temp_path = mktemp -d | path expand

			# Remove inline, pending, and touch files.
			let parsed = $test.path | path parse
			for path in (glob $'($parsed.parent | path join $parsed.stem){.{inline,pending,touched},/*.{pending,touched}}') {
				rm $path
			}

			# Run the test.
			let start = date now
			let timeout = $timeout | into int | $in / 1_000_000_000
			let output = with-env {
				TANGRAM_CONFIG: ($temp_path | path join "config.json"),
				TANGRAM_MODE: client,
				TMPDIR: $temp_path,
			} {
				let startup = '
					$env.config.display_errors.exit_code = true
				'
				if $no_capture {
					try {
						open /dev/null | timeout $timeout nu -e $startup $test.path o+e> /dev/tty
					}
					{ exit_code: $env.LAST_EXIT_CODE, stdout: '', stderr: '' }
				} else {
					open /dev/null | timeout $timeout nu -e $startup $test.path o+e>| complete
				}
			}
			let end = date now
			let duration = $end - $start

			# If the test passed, then delete snapshots which were not touched and remove touch files.
			if $output.exit_code == 0 {
				let parent_path = $test.path | path dirname
				let stem = $test.path | path parse | get stem
				for path in (glob $'($parent_path | path join $stem){.snapshot,/*.snapshot}') {
					if not ($path | str replace '.snapshot' '.touched' | path exists) {
						rm $path
					}
				}
				for path in (glob $'($parent_path | path join $stem){.touched,/*.touched}') {
					rm $path
				}
			}

			# Clean up the cloud resource.
			let ids_path = $temp_path | path join 'ids'
			let ids = if ($ids_path | path exists) {
				open $ids_path | lines | where { $in != '' }
			} else {
				[]
			}
			for id in $ids {
				cleanup $id
			}

			# Clean up the temp directory.
			chmod -R +w $temp_path
			rm -rf $temp_path

			# Send the result.
			let result = {
				duration: $duration,
				name: $test.name,
				output: $output,
			}
			$result | job send 0
		}
	}

	# Fill the worker pool.
	while ($running | length) < $jobs and ($pending | length) > 0 {
		let test = $pending | first
		$pending = $pending | skip 1
		let id = spawn $test
		$running = $running | append { id: $id, name: $test.name, start: (date now) }
	}

	# Spawn a job that sends a null message every 100ms to trigger progress updates.
	let interval_job = job spawn {
		loop {
			sleep 1sec
			null | job send 0
		}
	}

	# Hide the cursor.
	print -e -n "\e[?25l"

	# Process results as they complete.
	while ($running | length) > 0 or ($pending | length) > 0 {
		# Wait for the next event (either test completion or ticker).
		let result = job recv

		# Clear the from the cursor to the end.
		print -e -n "\e[0J"

		if $result != null {
			# Print the result.
			let symbol = if $result.output.exit_code == 0 {
				$'(ansi green)✓(ansi reset)'
			} else {
				$'(ansi red)✗(ansi reset)'
			}
			print -e $'($symbol) ($result.name) ($result.duration)'
			if $print_passing_test_output or $result.output.exit_code != 0 {
				print -e -n $result.output.stdout
			}

			# Store the result.
			$results = $results | append $result

			# Remove the completed job from the running list.
			$running = $running | where name != $result.name

			# Spawn a new job if there are more tests to run.
			if ($pending | length) > 0 {
				let test = $pending | first
				$pending = $pending | skip 1
				let id = spawn $test
				$running = $running | append { id: $id, name: $test.name, start: (date now) }
			}
		}

		if not $no_capture {
			# Print the running tests.
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

			# Print the progress bar.
			let completed = $results | length
			let passed = $results | where output.exit_code == 0 | length
			let failed = $results | where output.exit_code != 0 | length
			let ratio = if $total > 0 { $completed / $total } else { 0 }
			let filled = ($ratio * 10) | math floor
			let bar = if $filled > 0 { (1..$filled | each { '=' } | str join) + '>' } else { '>' }
			let bar = if $filled < 10 { $bar + (1..(9 - $filled) | each { ' ' } | str join) } else { $bar }
			let elapsed = ((date now) - $start) / 1sec | math floor | into duration -u sec
			let progress = $'[($bar)] ($completed)/($total): ($running | length) running, (ansi green)($passed) passed(ansi reset), (ansi red)($failed) failed(ansi reset), ($elapsed)'
			print -e -n $'($progress)'

			# Move the cursor up.
			print -e -n $"\r"
			if ($running | length) > 0 {
				print -e -n $"\e[($running | length)A"
			}
		}
	}

	job kill $interval_job

	# Clear.
	print -e -n "\e[0J"

	# Show the cursor.
	print -e -n "\e[?25h"

	if $review {
		for test in $tests {
			let parsed = $test.path | path parse

			for pending_path in (glob $'($parsed.parent | path join $parsed.stem){.{pending},/*.{pending}}') {
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

			# Delete snapshots which were not touched and remove touch files.
			if ($inline_paths | length) > 0 {
				let parent_path = $test.path | path dirname
				let stem = $test.path | path parse | get stem
				for path in (glob $'($parent_path | path join $stem){.snapshot,/*.snapshot}') {
					if not ($path | str replace '.snapshot' '.touched' | path exists) {
						rm $path
					}
				}
				for path in (glob $'($parent_path | path join $stem){.touched,/*.touched}') {
					rm $path
				}
			}
		}
	}

	# Print the summary.
	let passed = $results | where output.exit_code == 0 | length
	let failed = $results | where output.exit_code != 0 | length
	let total = $results | length
	print -e $'(ansi green)($passed) passed(ansi reset), (ansi red)($failed) failed(ansi reset), ($total) total'

	# Print the failed tests.
	if $failed > 0 {
		for result in ($results | where output.exit_code != 0) {
			print -e $'(ansi red)✗(ansi reset) ($result.name) ($result.duration)'
		}
	}

	if $failed > 0 {
		exit 1
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

	# Get the number of leading tabs to remove. Filter out lines that are empty or contain only tabs and spaces.
	let non_whitespace_lines = $lines | where { |line|
		let trimmed = $line | str trim
		($trimmed | str length) > 0
	}
	let leading_tabs_count = if ($non_whitespace_lines | length) > 0 {
		$non_whitespace_lines
			| each { |line|
				# Find the position of the first non-tab character.
				let chars = $line | split chars
				mut count = 0
				for char in $chars {
					if $char == "\t" {
						$count = $count + 1
					} else {
						break
					}
				}
				$count
			}
			| math min
	} else {
		0
	}

	# Remove the leading tabs from each line and combine them with newlines.
	let result = $lines
		| each { |line|
			if ($line | str length) >= $leading_tabs_count {
				$line | str substring $leading_tabs_count..
			} else {
				$line
			}
		}
		| str join "\n"

	$result
}

export def --env snapshot [
	--name (-n): string
	--path (-p)
	value: any
	inline?: string
] {
	if $inline != null {
		snapshot_inline --path=$path --span=(metadata $inline).span $value $inline
	} else {
		snapshot_file --name=$name --path=$path $value
	}
}

def --env snapshot_inline [
	--path (-p)
	--span: record
	value: any
	inline: string
] {
	let new_value = if $path {
		snapshot_path $value | to json -i 2
	} else {
		$value | to text
	}

	# Get the expected value by processing the snapshot with doc.
	let expected_value = doc $inline

	# If the values match, return early.
	if $new_value == $expected_value {
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
		new: $new_value,
	}

	$inline_entries | to json | save -f $inline_path

	error make {
		msg: 'the snapshot does not match',
		help: (diff $expected_value $new_value),
		label: {
			span: $span,
			text: 'the snapshot',
		},
	}
}

def --env snapshot_file [
	--name (-n): string
	--path (-p): string
	value: any
] {
	let new_value = if $path {
		snapshot_path $value | to json -i 2
	} else {
		$value | to text
	}

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
		$new_value | save -f $pending_path
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
	if $new_value != $old_value {
		$new_value | save -f $pending_path
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

export def --env spawn [
	--busybox
	--cloud
	--config (-c): record
	--name (-n): string
] {
	mut default_config = {
		advanced: {
			disable_version_check: true
			internal_error_locations: false
		},
		remotes: [],
	}

	mut id: any = null
	if $cloud {
		$id = random chars
		$id ++ "\n" | save --append ($env.TMPDIR | path join 'ids')
		print -e $id

		createdb -U postgres -h localhost $'database_($id)'
		psql -U postgres -h localhost -d $'database_($id)' -f ($repository_path | path join 'packages/server/src/database/postgres.sql')

		createdb -U postgres -h localhost $'index_($id)'
		for path in (ls ($repository_path | path join 'packages/server/src/index/postgres') | get name | sort) {
			psql -U postgres -h localhost -d $'index_($id)' -f $path
		}

		nats stream create $'index_($id)' --discard new --retention work --subjects $'($id).index' --defaults
		nats consumer create $'index_($id)' index --deliver all --max-pending 1000000 --pull --defaults

		cqlsh -e $"create keyspace \"store_($id)\" with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };"
		cqlsh -k $'store_($id)' -f packages/store/src/scylla.cql

		let config = {
			database: {
				kind: 'postgres',
				url: $'postgres://postgres@localhost:5432/database_($id)',
			},
			index: {
				kind: 'postgres',
				url: $'postgres://postgres@localhost:5432/index_($id)',
			},
			indexer: {
				message_batch_timeout: 1,
			},
			messenger: {
				kind: 'nats',
				id: $id,
				url: 'nats://localhost',
			},
			remotes: [],
			store: {
				kind: 'scylla',
				addr: 'localhost:9042',
				keyspace: $'store_($id)',
			},
			watchdog: {
				batch_size: 100,
				interval: 1,
				ttl: 60
			}
		}
		$default_config = $default_config | merge $config
	}

	# Write the config.
	let config = $default_config | merge deep --strategy append ($config | default {})
	let config_path = mktemp -d
	let config_path = $config_path | path join 'config.json'
	$config | to json | save -f $config_path

	# Create the directory.
	let directory_path = mktemp -d

	# Determine the url.
	let url = $'http+unix://($directory_path | url encode --all)%2Fsocket'
	$env.TANGRAM_URL = $url

	# Spawn the server.
	match $nu.os-info.name {
		'macos' => {
			job spawn {
				bash -c $"
					PARENT_PID=$PPID
					SELF_PID=$$
					\(
						while kill -0 $PARENT_PID 2>/dev/null; do
							sleep 1
						done
						kill -9 -$SELF_PID
					\) &
					tangram -c ($config_path) -d ($directory_path) -u ($url) serve
				" e>| lines | each { |line| print -e $"($name | default 'server'): ($line)\r" }
			}
		}
		'linux' => {
			job spawn {
				(
					setpriv --pdeathsig SIGKILL
					tangram -c $config_path -d $directory_path -u $url serve
				) e>| lines | each { |line| print -e $"($name | default 'server'): ($line)\r" }
			}
		}
	}

	loop {
		let output = tg health | complete
		if $output.exit_code == 0 {
			break;
		}
		sleep 10ms
	}

	# Tag busybox if requested.
	if $busybox {
		# Create a temp for busybox
		let busybox_path = mktemp -d
		let busybox_source = '
			const SOURCES: Record<string, { url: string, checksum: tg.Checksum }> = {
				"aarch64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2024.10.03/utils_universal_darwin.tar.zst",
					checksum: "sha256:7bd26e53a370d66eb05436c0a128d183a66dd2aba3c2524d94b916bd4515be40",
				},
				"x86_64-darwin": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2024.10.03/utils_universal_darwin.tar.zst",
					checksum: "sha256:7bd26e53a370d66eb05436c0a128d183a66dd2aba3c2524d94b916bd4515be40",
				},
				"aarch64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2024.10.03/utils_aarch64_linux.tar.zst",
					checksum: "sha256:486ef386ca587e5a3366df556da6140e9fd633462580a53c63942af411c9f40f",
				},
				"x86_64-linux": {
					url: "https://github.com/tangramdotdev/bootstrap/releases/download/v2024.10.03/utils_x86_64_linux.tar.zst",
					checksum: "sha256:dcbc2b66a046a66216f4c54d79f2a434c086346799f28b7f405bd6a2dc0e8543",
				},
			};

			export const env = (host?: string) => {
				const host_ = host ?? tg.process.env.TANGRAM_HOST;
				tg.assert(typeof host_ === "string");
				const kv = Object.entries(SOURCES).find(([k, _]) => k === host_);
				tg.assert(kv, `unknown host: ${host_}`);
				const { url, checksum } = kv[1];
				const dir = tg.download(url, checksum, { mode: "extract" }).then(tg.Directory.expect);
				return { PATH: tg.Mutation.suffix(tg`${dir}/bin`, ":") };
			};

			export default env;
		';
		$busybox_source | save ($busybox_path | path join 'tangram.ts')
		run tangram check $busybox_path
		run tangram -c ($config_path) tag 'busybox' $busybox_path
		run rm -rf $busybox_path
	}

	{ config: $config_path, directory: $directory_path, url: $url }
}

def cleanup [id: string] {
	# Drop the postgres databases.
	try { dropdb -U postgres -h localhost $'database_($id)' }
	try { dropdb -U postgres -h localhost $'index_($id)' }

	# Remove the NATS stream and consumer.
	try { nats consumer rm -f $'($id).index' index }
	try { nats stream rm -f $'($id).index' }

	# Drop the scylla keyspace.
	try { cqlsh -e $"drop keyspace \"store_($id)\";" }
}

export def --wrapped run [...command] {
	let output = ^$command.0 ...($command | skip 1) | complete
	if $output.exit_code != 0 {
		error make {
			msg: 'the process failed',
			label: {
				span: (metadata $command).span,
				text: 'the command',
			},
			help: $output.stderr,
		}
	}
	$output.stdout
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
	$line_prefix | parse --regex '^(\s*)' | get capture0.0? | default ''
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

def xattr_list [path: string] {
	match $nu.os-info.name {
		'macos' => { xattr $path | lines }
		'linux' => { getfattr -m '.' $path | complete | get stdout | lines | where { |l| not ($l starts-with '#') and $l != '' } }
	}
}

def xattr_read [name: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -p $name $path | str trim }
		'linux' => { getfattr -n $name --only-values $path | str trim }
	}
}

def xattr_write [name: string, value: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -w $name $value $path }
		'linux' => { setfattr -n $name -v $value $path }
	}
}
