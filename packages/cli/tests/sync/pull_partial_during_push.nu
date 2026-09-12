use ../../test.nu *

# A client that already has an object but not its child pulls the object with a sync token while a push of it is held. The pull requests the missing child and waits for the push instead of failing.

let root_token = random chars

# The remote stores one object per batch so that the hold on the blob leaves the file stored.
let store = { object_max_batch: 1 }
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob = tg --url $remote.url login --verbose --name bob | from json
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}

# Bob also holds a valid proof for an unrelated resource.
let unrelated = tg --url $remote.url --token $bob.token put 'tg.file("unrelated")' | str trim
tg --url $remote.url --token $root_token index
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let unrelated_token = (
	http get --headers { Accept: 'application/json', Authorization: $'Bearer ($bob.token)' } --unix-socket $socket $'http://localhost/objects/($unrelated)'
	| get tokens.local.authorization.0
)

# Alice has the directory and its file. Bob has the directory but not the file.
let directory = tg --url $alice_local.url put 'tg.directory({ "file": tg.file("hello") })' | str trim
let file = tg --url $alice_local.url put 'tg.file("hello")' | str trim
let blob = tg --url $alice_local.url put 'tg.blob("hello")' | str trim
let script = 'tg.directory({ "file": ' + $file + ' })'
let output = tg --url $bob_local.url put $script | str trim
assert equal $output $directory "bob should have the same directory"
failure (tg --url $bob_local.url get --local $file | complete) "bob should not have the file"

# Hold the blob's store write on the remote so that alice's push stays open.
let watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json)
	| from json
	| get watch
)

# Start alice's push and take the referent with the sync token from its log.
let push_log = $env.TMPDIR | path join push.log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $alice_local.url push $directory o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $watch 0 | complete
success $output "alice's push should reach the blob"
let push_lines = open --raw $push_log | lines | where {|line| $line =~ "sync" }
if ($push_lines | is-empty) {
	error make { msg: $"the push should log the referent with the sync token:\n(open --raw $push_log)" }
}
let referent = $push_lines | first | str trim
# Put the unrelated proof first so it cannot displace the root proof or imply a parent edge.
let referent = $referent | str replace 'tokens[remote][authorization][0]=' $'tokens[remote][authorization][0]=($unrelated_token | url encode --all)&tokens[remote][authorization][1]='

# Bob pulls the referent. The pull requests the file and waits for the blob.
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull $referent | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $pull --timeout 5sec } catch { null }
if $output != null {
	error make { msg: $"the pull should wait while the push is held: ($output)" }
}

# Release the blob. Both the push and the pull complete.
tg --url $remote.url --token $root_token checkpoint continue sync.get.store.object $watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $watch
success (job recv --tag $push --timeout 30sec) "alice's push should complete"
success (job recv --tag $pull --timeout 30sec) "bob's pull should complete"
let output = tg --url $bob_local.url get --local $file | complete
success $output "bob should have the file"
