use ../../lib/test.nu *

# Cleaning retains the contents of a tagged file produced by sync.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

let contents = 'checkout retained by its blob'
let blob = tg --url $remote.url put --no-tokens 'tg.blob("checkout retained by its blob")' | referent node
let file_value = ['tg.file({"contents":' $blob '})'] | str join
let file = tg --url $remote.url put --no-tokens $file_value | referent node
tg --url $local.url pull $file

tg --url $local.url tag checkout-pointer-clean $file
tg --url $local.url clean
server stop $remote
assert equal (tg --url $local.url read $blob) $contents
assert equal (tg --url $local.url read $file) $contents
