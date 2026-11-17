#!/usr/bin/env nu

# Fail if an integration test waits on time without saying why. A Nushell `sleep` must have a comment directly above it that says why real time is required.
def main [] {
	let root = $env.FILE_PWD
	let paths = (glob $'($root)/tests/**/*.nu') ++ (glob $'($root)/util/**/*.nu')
	mut failures = []
	for path in $paths {
		let lines = open --raw $path | lines
		for entry in ($lines | enumerate) {
			if ($entry.item !~ '^\s*sleep \(?[0-9.]+\s*(ns|us|ms|sec|min|hr)\b') {
				continue
			}
			let previous = if $entry.index == 0 { '' } else { $lines | get ($entry.index - 1) }
			if ($previous !~ '^\s*#') {
				$failures = $failures ++ [$'($path | path relative-to $root):($entry.index + 1): a sleep needs a comment directly above it that says why real time is required']
			}
		}
	}
	if ($failures | is-not-empty) {
		print -e ($failures | str join "\n")
		exit 1
	}
}
