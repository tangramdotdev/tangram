use ../../test.nu *

# A build with --local=false runs on the default remote rather than on the local server.

let remote = spawn --name remote
let local = spawn --name local

tg remote put default $remote.url

let path = artifact {
	tangram.ts: 'export default function () { return 42; }'
}

let id = tg build --local=false --detach $path
let output = tg wait $id | from json
assert equal $output.exit 0 "the build should succeed"

let seen = tg --url $remote.url process get $id | complete
success $seen "the process should exist on the remote"
