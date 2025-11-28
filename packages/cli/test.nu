#!/usr/bin/env nu

use std/util 'path add'

export use std assert

const path = path self '../../'

def main [
	--jobs (-j): int # The number of concurrent tests to run.
	--review (-r) # Review snapshots.
	--print-output # Print the output of passing tests.
	filter: string = '.*' # Filter tests.
] {
	# Add the debug build to the path.
	cargo build
	ln -sf tangram target/debug/tg
	path add ($path | path join 'target/debug')

	# Get the matching tests.
	let tests_path = ($path | path join 'packages/cli/tests')
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

	def spawn [test: record] {
		job spawn {
			# Create a temp directory for this test.
			let temp_path = mktemp -d | path expand

			# Remove pending and touch files.
			let parsed = $test.path | path parse
			for path in (glob $'($parsed.parent | path join $parsed.stem){.{pending,touched},/*.{pending,touched}}') {
				rm $path
			}

			# Run the test.
			let start = date now
			let output = with-env {
				TANGRAM_CONFIG: ($temp_path | path join "config.json"),
				TANGRAM_MODE: client,
				TMPDIR: $temp_path,
			} { open /dev/null | nu $test.path o+e>| complete }
			let end = date now
			let duration = $end - $start

			# If the test passed, then delete snapshots which were not touched.
			if $output.exit_code == 0 {
				let parent_path = $test.path | path dirname
				let stem = $test.path | path parse | get stem
				for path in (glob $'($parent_path | path join $stem){.snapshot,/*.snapshot}') {
					if not ($path | str replace '.snapshot' '.touched' | path exists) {
						rm $path
					}
				}
			}

			# Remove touch files.
			let parsed = $test.path | path parse
			for path in (glob $'($parsed.parent | path join $parsed.stem){.touched,/*.touched}') {
				rm $path
			}

			# Cleanup the temp directory.
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
			if $print_output or $result.output.exit_code != 0 {
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

		# Save the cursor position.
		print -e -n "\e[s"

		# # Print the running tests.
		# for run in $running {
		# 	let duration = ((date now) - $run.start) / 1sec | math floor | into duration -u sec
		# 	print -e $'(ansi blue)●(ansi reset) ($run.name) ($duration)'
		# }

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

		# Restore the cursor position.
		print -e -n "\e[u"
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
					delta --file-style=omit --hunk-header-style=omit --no-gitconfig $snapshot_path $pending_path | print -e
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
				mkdir $path | ignore
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
					xattr-write $pair.key $pair.value $path
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

	# Remove the first and last lines if they are empty or contain only whitespace.
	if ($lines | length) > 0 and (($lines | first | str trim | str length) == 0) {
		$lines = $lines | skip 1
	}
	if ($lines | length) > 0 and (($lines | last | str trim | str length) == 0) {
		$lines = $lines | drop
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
	value: any
	--name (-n): string
	--path (-p)
] {
	# Get the snapshot path.
	let test_path = $env.CURRENT_FILE
	let test_name = $test_path | path parse | get stem
	let test_directory_path = $test_path | path dirname
	let snapshot_directory_path = $test_directory_path | path join $test_name
	if $name != null {
		mkdir -v $snapshot_directory_path | ignore
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

	# Snapshot.
	let new_value = if $path {
		snapshot_path $value | to json -i 2
	} else {
		$value | to text
	}

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
		let help = (
			delta
				--file-style=omit
				--hunk-header-style=omit
				--no-gitconfig
				$snapshot_path
				$pending_path
		) | complete | get stdout
		error make {
			msg: 'the snapshot does not match',
			label: {
				span: (metadata $value).span,
				text: 'the value',
			},
			help: $help,
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
		let names = xattr-list $path | where { |name| $name starts-with 'user.tangram' }
		let xattrs = $names | reduce -f {} { |name, acc| $acc | insert $name (xattr-read $name $path) }
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
	--config (-c): record
	--name (-n): string
] {
	let default_config = {
		advanced: {
			disable_version_check: true
			internal_error_locations: false
		},
		remotes: [],
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

	{ config: $config_path, directory: $directory_path, url: $url }
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

def xattr-list [path: string] {
	match $nu.os-info.name {
		'macos' => { xattr $path | lines }
		'linux' => { getfattr -m '.' $path | complete | get stdout | lines | where { |l| not ($l starts-with '#') and $l != '' } }
	}
}

def xattr-read [name: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -p $name $path | str trim }
		'linux' => { getfattr -n $name --only-values $path | str trim }
	}
}

def xattr-write [name: string, value: string, path: string] {
	match $nu.os-info.name {
		'macos' => { xattr -w $name $value $path }
		'linux' => { setfattr -n $name -v $value $path }
	}
}
