use ../../test.nu *

# A build with --remote=false selects no remotes rather than failing to resolve a location.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}

let output = tg build --remote=false $path
assert equal $output '42' "the build should run with no remotes"
