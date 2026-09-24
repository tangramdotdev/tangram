use ../lib/test.nu *

# A source-less pull must wait for a local sync and preserve its proven access.
let root_token = random chars
let store = { object_concurrency: 8, object_max_batch: 1 }
let destination = server spawn --name destination --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: {
		control: { index_timeout: 5 },
		get: { store: { lmdb: $store, memory: $store, scylla: $store } },
	},
}
let alice = tg --url $destination.url login --verbose --name alice | from json
let bob = tg --url $destination.url login --verbose --name bob | from json
let source = server spawn --name source --config {
	remotes: { default: { token: $alice.token, url: $destination.url } },
}
let file = tg --url $source.url put --no-tokens 'tg.file("private")' | str trim
let file_watch = tg --url $destination.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $file } | to json --raw) | from json | get watch
let ack_watch = tg --url $destination.url --token $root_token checkpoint watch sync.control.ack --params ({ node: $file } | to json --raw) | from json | get watch
let push_log = $env.TMPDIR | path join push.log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $source.url push $file o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $destination.url --token $root_token checkpoint wait sync.get.store.object $file_watch 0 | ignore
wait_until { (open --raw $push_log) =~ 'tokens\[remote\]\[authorization\][^\r\n]*\r?\n' } 'the push should log its sync token'
let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]\[authorization\]' } | first | str trim
let referent = $referent | str replace --all 'tokens[remote]' 'tokens[local]'
let socket = $destination.url | str replace 'http+unix://' '' | url decode
let pull = job spawn {
	let job_id = job id
	let response = http post --max-time 15sec --raw --content-type application/json --headers { Authorization: $'Bearer ($bob.token)' } --unix-socket $socket http://localhost/pull { nodes: [$referent] }
	$response | job send --tag $job_id 0
}
success (timeout 10s tg --url $destination.url --token $root_token checkpoint wait sync.control.ack $ack_watch 0 | complete) 'the pull should wait on the local sync'
tg --url $destination.url --token $root_token checkpoint unwatch sync.control.ack $ack_watch
tg --url $destination.url --token $root_token checkpoint unwatch sync.get.store.object $file_watch
let response = job recv --tag $pull --timeout 15sec
assert ($response | str contains 'event: output') 'the source-less pull should complete'
assert not ($response | str contains 'event: error') 'the source-less pull should not fail'
success (tg --url $destination.url --token $bob.token read $file | complete) 'the pull should persist access without requiring the sync token again'
success (job recv --tag $push --timeout 10sec) 'the push should complete'

# Stored bytes alone are not proof, and no source must not silently select a remote.
let eve = tg --url $destination.url login --verbose --name eve | from json
let response = http post --max-time 15sec --raw --content-type application/json --headers { Authorization: $'Bearer ($eve.token)' } --unix-socket $socket http://localhost/pull { nodes: [$file] }
assert ($response | str contains 'event: error') 'a source-less pull without proof should time out'
failure (tg --url $destination.url --token $eve.token read $file | complete) 'the failed pull should not grant access'
