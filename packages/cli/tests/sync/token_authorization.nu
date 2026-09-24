use ../lib/test.nu *

# A sync token authorizes only the nodes and permissions proven by its sync graph.
let root_token = random chars
let store = { object_concurrency: 8, object_max_batch: 1 }
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: {
		control: { index_timeout: 1, recovery_timeout: 1, request_timeout: 1 },
		get: { store: { lmdb: $store, memory: $store, scylla: $store } },
	},
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
	sync: { control: { index_timeout: 1 } },
}

# Hold Alice's private object before storage and leave a second object held to keep the sync open.
let private = tg --url $alice_local.url put 'tg.file("private")' | str trim
let blocker = tg --url $alice_local.url put 'tg.file("blocker")' | str trim
let private_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $private } | to json --raw) | from json | get watch
let blocker_watch = tg --url $remote.url --token $root_token checkpoint watch sync.get.store.object --params ({ id: $blocker } | to json --raw) | from json | get watch
let ack_watch = tg --url $remote.url --token $root_token checkpoint watch sync.control.ack --params ({ node: $private } | to json --raw) | from json | get watch
let push_log = $env.TMPDIR | path join push.log
let push = job spawn {
	let job_id = job id
	let output = tg --no-quiet --url $alice_local.url push $private $blocker o+e>| tee { save --force $push_log } | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $private_watch 0 | ignore
wait_until { (open --raw $push_log) =~ 'tokens\[remote\]\[authorization\][^\r\n]*\r?\n' } 'the push should log its complete sync token'
let referent = open --raw $push_log | lines | where {|line| $line =~ 'tokens\[remote\]\[authorization\]' } | first | str trim
let sync = $'http://localhost/($referent)' | url parse | get params | where key == 'tokens[remote][authorization][0]' | first | get value

# Bob's request is retained, then succeeds with the permissions reported when the object is stored.
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let query = { 'tokens[local][authorization][0]': $sync } | url build-query
let read = job spawn {
	let job_id = job id
	let output = http get --max-time 30sec --unix-socket $socket --headers {
		Accept: 'application/json',
		Authorization: $'Bearer ($bob.token)',
	} $'http://localhost/objects/($private)?($query)'
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.control.ack $ack_watch 0 | ignore
tg --url $remote.url --token $root_token checkpoint unwatch sync.control.ack $ack_watch
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $private_watch
job recv --tag $read --timeout 10sec | ignore
timeout 10s tg --url $remote.url --token $root_token checkpoint wait sync.get.store.object $blocker_watch 0 | ignore
tg --url $remote.url --token $root_token checkpoint unwatch sync.get.store.object $blocker_watch
success (job recv --tag $push --timeout 10sec) "Alice's push must finish"

# Polling must not expose stored bytes without authorization.
failure (tg --url $remote.url --token $bob.token get --local $private | complete) "storage alone must not authorize Bob's read"

# A sync token and authorization proofs for unrelated nodes do not authorize Alice's object.
let unrelated_object = tg --url $bob_local.url put 'tg.file("unrelated")' | str trim
let unrelated_referent = tg --url $bob_local.url push $unrelated_object | str trim
let unrelated_uri = $'http://localhost/($unrelated_referent)' | url parse
let unrelated_sync = $unrelated_uri.params | where key == 'tokens[remote][authorization][0]' | first | get value
let unrelated = [one two] | each {|name|
	let value = ['tg.file("' $name '")'] | str join
	tg --url $remote.url --token $bob.token put $value | str trim
}
tg --url $remote.url --token $root_token index
let tokens = $unrelated | each {|id|
	http get --headers { Accept: 'application/json', Authorization: $'Bearer ($bob.token)' } --unix-socket $socket $'http://localhost/objects/($id)'
	| get tokens.local.authorization.0
}
let query = {
	'tokens[remote][authorization][0]': ($tokens | get 0),
	'tokens[remote][authorization][1]': ($tokens | get 1),
	'tokens[remote][authorization][2]': $unrelated_sync,
} | url build-query
let output = timeout 10s tg --url $bob_local.url pull $'($private)?($query)' | complete
assert ($output.exit_code != 124) "the unauthorized pull should finish"
failure $output "the unrelated proofs must not authorize the private object"
failure (tg --url $bob_local.url get --local $private | complete) "the private object must not be copied"
