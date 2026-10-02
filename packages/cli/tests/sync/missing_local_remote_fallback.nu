use ../lib/test.nu *

# A missing local object is fetched from a configured remote.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },

}
let object = tg --url $remote.url put --no-tokens 'tg.file("remote")' | referent node
success (timeout 5s tg --url $local.url get $object | complete) 'a remote hit must succeed despite a local miss'
