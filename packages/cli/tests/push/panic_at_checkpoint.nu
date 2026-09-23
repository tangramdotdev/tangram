use ../lib/test.nu *

# A registered checkpoint panic fails the request that hits the checkpoint.
let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token } },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let file = tg --url $local.url put 'tg.file("hello")' | str trim

tg --url $remote.url --token $root_token checkpoint panic sync.request.response

let output = tg --url $local.url push $file | complete
failure $output
