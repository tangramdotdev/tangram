use ../../test.nu *

let remote = spawn --cloud -n remote

# Tag the referent on the remote server.
let referent_path = artifact {
	tangram.ts: '
		export default () => "foo";
	'
}
tg -u $remote.url tag foo $referent_path

# Create a local server with the remote configured.
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Check in the referrer on the local server.
let referrer_path = artifact {
	tangram.ts: '
		import foo from "foo";
		export default () => foo();
	'
}

let id1 = tg checkin $referrer_path
tg index
let output1 = tg object get --blobs --depth=inf --pretty $id1

# Stop and recreate the local server (simulates a clean restart). This tests that the lockfile is correctly written and read back.
let local2 = spawn -n local2 -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Check in the same artifact again on the new local server.
let id2 = tg -u $local2.url checkin $referrer_path
tg -u $local2.url index
let output2 = tg -u $local2.url object get --blobs --depth=inf --pretty $id2

# Confirm that the two outputs are the same.
assert ($output1 == $output2) "the checkin should produce the same output after clean"
