use ../../util/test.nu *
use ../../util/release.nu
use ../../../../scripts/release/stage.nu

# A pull without a target stages every target that is tagged on the remote, and fails when it cannot pull one of them.
let remote = server spawn --name remote
let path = release fixture
cd $path
let commit = ^git rev-parse HEAD | str trim

# Tag a target on the remote.
def tag [target: string, commit: string, url: string] {
	let directory = artifact {($'tangram_($target)'): $'binary for ($target)'}
	stage record $directory $target $commit
	let id = tg --url $url checkin --no-tokens $directory | referent node
	tg --url $url tag put --create-ancestors $'tangram/release/($commit)/($target)' $id
}

# Pull the only target.
tag x86_64-linux-gnu $commit $remote.url
let local = server spawn --name local --config {remotes: {default: {url: $remote.url}}}
let output = tg run .#release -- --stage pull | complete
success $output
assert equal (open --raw release/tangram_x86_64-linux-gnu) 'binary for x86_64-linux-gnu'
assert not ('release/tangram_aarch64-linux-gnu' | path exists)

# Fail when a tagged target cannot be pulled, rather than stage only the other target. The error is the same as that for a target that is not tagged.
tag aarch64-linux-gnu $commit $remote.url
let tg_path = which tg | get 0.path
let script = [
	'#!/bin/sh'
	'case "$*" in'
	'"pull --force"*/aarch64-linux-gnu) echo "failed to get the reference" >&2; exit 1 ;;'
	'esac'
	$'exec "($tg_path)" "$@"'
	''
] | str join "\n"
let bin = artifact {tg: (file --executable $script)}
let output = with-env {PATH: ($env.PATH | prepend $bin)} { tg run .#release -- --stage pull | complete }
failure $output
assert ($output.stderr | str contains 'failed to pull')
assert not ('release/tangram_aarch64-linux-gnu' | path exists)

# Fail when the remote cannot be reached.
server stop $remote
let output = tg run .#release -- --stage pull | complete
failure $output
assert ($output.stderr | str contains 'failed to list the release targets')

# Pull the target that was tagged after the first pull.
let remote = do { server start $remote }
let output = tg run .#release -- --stage pull | complete
success $output
assert equal (open --raw release/tangram_aarch64-linux-gnu) 'binary for aarch64-linux-gnu'

# Report a release that has no targets.
^git -c user.name=test -c user.email=test@example.com -c commit.gpgsign=false commit -q --allow-empty -m empty
let output = tg run .#release -- --stage pull | complete
failure $output
assert ($output.stderr | str contains 'failed to list the release targets')
