use ../../test.nu *

# A lazy file pull materializes its contents under the file ID and stores the leaf through a checkout pointer.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

let contents = 'pulled through a checkout pointer'
let blob = tg --url $remote.url put 'tg.blob("pulled through a checkout pointer")' | str trim
let file = tg --url $remote.url put 'tg.file("pulled through a checkout pointer")' | str trim
let executable = (
	tg --url $remote.url put 'tg.file({ "contents": tg.blob("pulled through a checkout pointer"), "executable": true })'
	| str trim
)
let eager_file = tg --url $remote.url put 'tg.file("eager checkout pointer")' | str trim
let eager_skipped = (
	tg --url $remote.url put 'tg.file({ "contents": tg.blob("pulled through a checkout pointer"), "module": "ts" })'
	| str trim
)
let large_bytes = random binary 5000000
let large_blob = $large_bytes | tg --url $remote.url write | str trim
let large_file_value = ['tg.file({"contents":' $large_blob '})'] | str join
let large_file = (
	tg --url $remote.url put $large_file_value
	| str trim
)
let large_executable_value = (
	['tg.file({"contents":' $large_blob ',"executable":true})'] | str join
)
let large_executable = (
	tg --url $remote.url put $large_executable_value
	| str trim
)
let large_module_value = ['tg.file({"contents":' $large_blob ',"module":"ts"})'] | str join
let large_module = (
	tg --url $remote.url put $large_module_value
	| str trim
)

tg pull $file

let path = $local.checkout_directory | path join $file
assert ($path | path exists) 'expected the pull to materialize the file in the checkouts directory'
assert equal (open --raw $path) $contents

tg pull $eager_file --eager
let eager_path = $local.checkout_directory | path join $eager_file
assert equal (open --raw $eager_path) 'eager checkout pointer'

# Removing the checkout makes the leaf unreadable, proving its bytes were not retained in the store.
rm $path
let output = tg read $blob | complete
failure $output 'expected the leaf to require its checkout pointer'

# A skipped local subtree is copied into a checkout when the setting is enabled later.
let skipped = server spawn --name skipped --config {
	remotes: { default: { url: $remote.url } },
	sync: { get: { checkout_pointers: false } },
}
tg --url $skipped.url pull $file
tg --url $skipped.url pull $large_file
let disabled_path = $skipped.checkout_directory | path join $file
let large_disabled_path = $skipped.checkout_directory | path join $large_file
assert (not ($disabled_path | path exists)) 'expected checkout pointers to be configurable'
assert (not ($large_disabled_path | path exists)) 'expected checkout pointers to be configurable'
assert equal (
	tg --url $skipped.url read $blob
	| str trim
) $contents 'expected disabled checkout pointers to retain leaf bytes in the store'

server stop $skipped
open $skipped.config_path
| upsert sync.get.checkout_pointers true
| to json
| save --force $skipped.config_path
let skipped = server start $skipped

tg --url $skipped.url pull $executable
let skipped_path = $skipped.checkout_directory | path join $executable
assert ($skipped_path | path exists) 'expected the skipped file to be materialized'
assert equal (open --raw $skipped_path) $contents

tg --url $skipped.url pull $eager_skipped --eager
let eager_skipped_path = $skipped.checkout_directory | path join $eager_skipped
assert equal (open --raw $eager_skipped_path) $contents

# Exercise batched existing-leaf loads and cached checkout-source handles across multiple batches.
tg --url $skipped.url pull $large_executable
let large_executable_path = $skipped.checkout_directory | path join $large_executable
assert equal (open --raw $large_executable_path | hash sha256) ($large_bytes | hash sha256)
tg --url $skipped.url pull $large_module --eager
let large_module_path = $skipped.checkout_directory | path join $large_module
assert equal (open --raw $large_module_path | hash sha256) ($large_bytes | hash sha256)

rm $skipped_path
rm $eager_skipped_path
rm $large_executable_path
rm $large_module_path
let output = tg --url $skipped.url read $blob | complete
failure $output 'expected the copied leaf bytes to be removed from the store'

# A directly pulled multi-leaf blob uses the same default file identity as tg write.
let branch = server spawn --name branch --config {
	remotes: { default: { url: $remote.url } },
}
let bytes = random binary 300000
let branch_blob = $bytes | tg --url $remote.url write | str trim
tg --url $branch.url pull $branch_blob
let entries = (
	ls $branch.checkout_directory
	| where { |entry| ($entry.name | path basename | str starts-with 'fil_') }
)
assert equal ($entries | length) 1
let branch_path = $entries.0.name
assert equal (open --raw $branch_path | hash sha256) ($bytes | hash sha256)

let writer = server spawn --name writer
let written_blob = $bytes | tg --url $writer.url write | str trim
assert equal $written_blob $branch_blob
let written_file = (
	ls $writer.checkout_directory
	| where { |entry| ($entry.name | path basename | str starts-with 'fil_') }
	| get 0.name
)
assert equal ($written_file | path basename) ($branch_path | path basename)

server stop $remote
rm $branch_path
let output = tg --url $branch.url get --bytes $branch_blob | complete
success $output 'expected the branch bytes to remain in the store'
let output = tg --url $branch.url read $branch_blob | complete
failure $output 'expected the multi-leaf blob to require its checkout pointer'
