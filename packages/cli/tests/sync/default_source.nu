use ../lib/test.nu *

# An absent source selects the default remote for both push and pull.
let source = server spawn --name source
let destination = server spawn --name destination
let local = server spawn --name local --config {
	remotes: {
		default: { url: $source.url },
		destination: { url: $destination.url },
	},
}
let socket = $local.url | str replace 'http+unix://' '' | url decode
for operation in [push pull] {
	for explicit_null in [false true] {
		let value = $'($operation)-($explicit_null)'
		let id = tg --url $source.url put ('tg.blob(' + ($value | to json --raw) + ')') | str trim
		let target = if $operation == push { 'remote:destination' } else { 'local' }
		let arg = { destination: $target, nodes: [$id] }
		let arg = if $explicit_null { $arg | insert source null } else { $arg }
		let response = http post --raw --max-time 20sec --content-type application/json --unix-socket $socket --headers { Accept: 'text/event-stream' } $'http://localhost/($operation)' $arg
		assert not ($response | str contains 'event: error') 'the transfer should use the default remote source'
		let target_url = if $operation == push { $destination.url } else { $local.url }
		let target_socket = $target_url | str replace 'http+unix://' '' | url decode
		let data = http get --max-time 10sec --unix-socket $target_socket --headers { Accept: 'application/json' } $'http://localhost/objects/($id)' | get data.value.bytes | decode base64 | decode utf-8
		assert equal $data $value
	}
}

# An absent push destination selects the default remote.
for explicit_null in [false true] {
	let value = $'destination-($explicit_null)'
	let id = tg --url $local.url put ('tg.blob(' + ($value | to json --raw) + ')') | str trim
	let arg = { source: 'local', nodes: [$id] }
	let arg = if $explicit_null { $arg | insert destination null } else { $arg }
	let response = http post --raw --max-time 20sec --content-type application/json --unix-socket $socket --headers { Accept: 'text/event-stream' } 'http://localhost/push' $arg
	assert not ($response | str contains 'event: error') 'the transfer should use the default remote destination'
	let source_socket = $source.url | str replace 'http+unix://' '' | url decode
	let data = http get --max-time 10sec --unix-socket $source_socket --headers { Accept: 'application/json' } $'http://localhost/objects/($id)' | get data.value.bytes | decode base64 | decode utf-8
	assert equal $data $value
}

# The CLI leaves default remote selection to the server.
let pushed = tg --url $local.url put 'tg.blob("cli-push")' | str trim
tg --url $local.url push $pushed
let source_socket = $source.url | str replace 'http+unix://' '' | url decode
let data = http get --max-time 10sec --unix-socket $source_socket --headers { Accept: 'application/json' } $'http://localhost/objects/($pushed)' | get data.value.bytes | decode base64 | decode utf-8
assert equal $data 'cli-push'
let pulled = tg --url $source.url put 'tg.blob("cli-pull")' | str trim
tg --url $local.url pull $pulled
let data = http get --max-time 10sec --unix-socket $socket --headers { Accept: 'application/json' } $'http://localhost/objects/($pulled)' | get data.value.bytes | decode base64 | decode utf-8
assert equal $data 'cli-pull'
