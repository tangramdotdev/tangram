use ../../test.nu *

# A pull of an object whose push is still in progress waits for the pushed children to arrive instead
# of failing. The pusher confers the in-flight object with the referent the push returns, whose tokens
# authorize the puller on the remote and identify the incoming sync.

let root_token = random chars
# The remote stores one object per batch so that every object before the held blob is stored.
let store = { object_max_batch: 1 }
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { get: { store: { lmdb: $store, memory: $store, scylla: $store } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Bob also holds a valid proof for an unrelated resource.
let unrelated = tg --url $remote.url --token $bob.token put 'tg.file("unrelated")' | str trim
tg --url $remote.url --token $root_token index
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let unrelated_token = (
	http get --headers { Accept: 'application/json', Authorization: $'Bearer ($bob.token)' } --unix-socket $socket $'http://localhost/objects/($unrelated)'
	| get tokens.local.authorization.0
)

# Alice creates a directory whose file's blob is the last object the push sends.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
let blob = tg --url $alice_local.url put 'tg.blob("hello")' | str trim
tg --url $alice_local.url index

# Hold the blob's store write on the remote so the push stays open after the directory and the file are
# stored.
let blob_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blob } | to json)
	| from json
	| get watch
)

# Alice pushes the directory. The push logs the referent with the sync token as soon as the remote
# starts the sync, so Alice can confer it to Bob before the push finishes.
let push_log = $env.TMPDIR | path join push.log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $alice_local.url push $directory o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $blob_watch 0 | ignore
let push_lines = open --raw $push_log | lines | where {|line| $line =~ "sync" }
if ($push_lines | is-empty) {
	error make { msg: $"the push should log the referent with the sync token:\n(open --raw $push_log)" }
}
let referent = $push_lines | first | str trim
# Put the unrelated proof first so it cannot displace the root proof or imply a parent edge.
let referent = $referent | str replace 'tokens[remote][authorization][0]=' $'tokens[remote][authorization][0]=($unrelated_token | url encode --all)&tokens[remote][authorization][1]='

# The directory and the file are stored on the remote while the blob is held.
let stored = tg --url $remote.url --token $root_token object get $directory | complete
success $stored "the directory should be stored on the remote while the blob is held"

# Watch the remote's store read of the blob so the test can prove the pull reaches the held object.
let queue_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sync.put.store.object --params ({ id: $blob } | to json)
	| from json
	| get watch
)

# Bob pulls the referent Alice conferred while the push is held.
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull $referent | complete
	$output | job send --tag $job_id 0
}

# The pull reaches the blob before it has arrived.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sync.put.store.object $queue_watch 0 | complete
if $output.exit_code != 0 {
	let pull_output = try { job recv --tag $pull --timeout 1sec } catch { null }
	error make { msg: $"the pull should reach the held blob while the push is in progress:\n($pull_output)" }
}
tg --url $remote.url --token $root_token checkpoint continue sync.put.store.object $queue_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sync.put.store.object $queue_watch

# The pull waits for the blob rather than finishing without it.
let output = try { job recv --tag $pull --timeout 3sec } catch { null }
if $output != null {
	error make { msg: $"the pull should wait while the push is held: ($output)" }
}

# Release the blob so the push completes.
tg --url $remote.url --token $root_token checkpoint continue sync.get.store.object $blob_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $blob_watch
let output = job recv --tag $push --timeout 30sec
success $output "the push should complete after the blob is released"

# The pull completes with the whole directory.
let output = job recv --tag $pull --timeout 30sec
success $output "the pull should complete after the push finishes"
let file = tg --url $bob_local.url children $directory | from json | get 0
let output = tg --url $bob_local.url read $file | complete
success $output "Bob should read the pulled file"
snapshot ($output.stdout | str trim) 'hello'
