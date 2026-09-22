use ../lib/test.nu *

# Starting a failed pull must not grant access to a private object already on the destination.
let source = server spawn --name source --config { sync: { control: { index_timeout: 1 } } }
let root_token = random chars
let destination = server spawn --cloud --name destination --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	sync: { control: { index_timeout: 1, recovery_timeout: 1, request_timeout: 1 } },
}
let alice = tg --url $destination.url login --verbose --name alice | from json
let bob = tg --url $destination.url login --verbose --name bob | from json
let private = tg --url $destination.url --token $alice.token put 'tg.file("private")' | str trim
tg --url $destination.url --token $root_token index
failure (tg --url $destination.url --token $bob.token get --local $private | complete) "Bob should initially lack access"
tg --url $destination.url --token $bob.token remote put default $source.url

# Call the pull endpoint directly so the CLI does not first try to resolve the private object.
let socket = $destination.url | str replace 'http+unix://' '' | url decode
let response = http post --max-time 10sec --raw --content-type application/json --headers { Authorization: $'Bearer ($bob.token)' } --unix-socket $socket http://localhost/pull { nodes: [$private], source: 'remote' }
assert ($response | str contains 'event: error') "the pull from the empty source should fail"
let logs = $response | split row "\n\n" | where {|event| $event starts-with 'event: log' } | each {|event|
	$event | lines | where {|line| $line starts-with 'data:' } | first | str substring 5.. | from json
}
assert not ($logs | is-empty) "the pull should publish its sync token"
for log in $logs {
	let referent = $log.message
	let params = $'http://localhost/($referent)' | url parse | get params
	assert ($params | any {|param| $param.key == 'tokens[local][sync][0]' }) "the referent should identify the sync"
	assert not ($params | any {|param| $param.key =~ 'authorization' }) "starting a sync must not mint authorization tokens"
	failure (tg --url $destination.url --token $bob.token read $referent | complete) "the sync token must not authorize Bob"
}
failure (tg --url $destination.url --token $bob.token get --local $private | complete) "the failed pull must not grant access"
