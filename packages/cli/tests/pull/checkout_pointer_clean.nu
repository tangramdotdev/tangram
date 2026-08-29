use ../../test.nu *

# Cleaning retains a checkout referenced by a live blob produced by sync.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

let contents = 'checkout retained by its blob'
let blob = tg --url $remote.url put 'tg.blob("checkout retained by its blob")' | str trim
let file_value = ['tg.file({"contents":' $blob '})'] | str join
let file = tg --url $remote.url put $file_value | str trim
tg --url $local.url pull $file

let path = $local.checkout_directory | path join $file
assert equal (open --raw $path) $contents

tg --url $local.url tag checkout-pointer-clean $file
tg --url $local.url clean
assert ($path | path exists) 'expected cleaning to retain a checkout referenced by a live blob'
assert equal (open --raw $path) $contents
