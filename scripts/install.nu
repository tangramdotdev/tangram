#!/usr/bin/env nu

const repository_path = path self '../'

# Build and install the CLI with a tg symlink.
def main [
	--debug # Use a debug build instead of a release build.
	--directory: path = ~/.local/bin # The directory in which to install the CLI.
] {
	# Build the CLI.
	cd $repository_path
	let profile = if $debug { 'debug' } else { 'release' }
	let profile_args = if $debug { ['--profile', 'debug'] } else { ['--release'] }
	let commit = git rev-parse HEAD | str trim
	with-env { TANGRAM_CLI_COMMIT_HASH: $commit } {
		cargo build --all-features ...$profile_args --package tangram_cli --bin tangram
	}
	let target_path = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let source_path = $target_path | path join $profile tangram

	# Install the binary and the symlink.
	let directory = $directory | path expand
	let binary_path = $directory | path join tangram
	let link_path = $directory | path join tg
	for path in [$binary_path $link_path] {
		if ($path | path type) == dir {
			error make { msg: $"the install destination is a directory: ($path)" }
		}
	}
	mkdir $directory
	^install -m 755 $source_path $binary_path
	^ln -sfn tangram $link_path

	let symbol = if (is-terminal --stderr) and ($env.NO_COLOR? == null) {
		$"(ansi green)✓(ansi reset)"
	} else {
		'✓'
	}
	print -e $"($symbol) installed ($binary_path)"
	print -e $"($symbol) linked ($link_path) -> tangram"
}
