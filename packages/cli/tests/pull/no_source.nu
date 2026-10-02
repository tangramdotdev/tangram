use ../lib/test.nu *

# An omitted source selects the default remote.

let remote = server spawn --name remote
let file = tg --url $remote.url put --no-tokens 'tg.file("hello")' | referent node
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let socket = $local.url | str replace 'http+unix://' '' | url decode
let response = http post --max-time 15sec --raw --content-type application/json --unix-socket $socket http://localhost/pull { nodes: [$file] } | stream header
assert ($response.stream | str contains 'event: output') 'the pull should complete using the default remote'
assert not ($response.stream | str contains 'event: error') 'the pull should not fail'
server stop $remote
assert equal (tg --url $local.url read $file) 'hello'
