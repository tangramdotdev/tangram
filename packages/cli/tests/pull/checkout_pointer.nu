use ../lib/test.nu *

# A lazy file pull materializes its contents under the file ID and stores the leaf through a checkout pointer.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

let contents = 'pulled through a checkout pointer'
let blob = tg --url $remote.url put --no-tokens 'tg.blob("pulled through a checkout pointer")' | referent node
let file = tg --url $remote.url put --no-tokens 'tg.file("pulled through a checkout pointer")' | referent node
let executable = (
	tg --url $remote.url put --no-tokens 'tg.file({ "contents": tg.blob("pulled through a checkout pointer"), "executable": true })' | referent node
	| str trim
)
let eager_file = tg --url $remote.url put --no-tokens 'tg.file("eager checkout pointer")' | referent node
let eager_skipped = (
	tg --url $remote.url put --no-tokens 'tg.file({ "contents": tg.blob("pulled through a checkout pointer"), "module": "typescript" })' | referent node
	| str trim
)
let large_bytes = random binary 5000000
let large_blob = $large_bytes | tg --url $remote.url write --no-tokens | referent node
let large_file_value = ['tg.file({"contents":' $large_blob '})'] | str join
let large_file = (
	tg --url $remote.url put --no-tokens $large_file_value | referent node
	| str trim
)
let large_executable_value = (
	['tg.file({"contents":' $large_blob ',"executable":true})'] | str join
)
let large_executable = (
	tg --url $remote.url put --no-tokens $large_executable_value | referent node
	| str trim
)
let large_module_value = ['tg.file({"contents":' $large_blob ',"module":"typescript"})'] | str join
let large_module = (
	tg --url $remote.url put --no-tokens $large_module_value | referent node
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
let local_skipped = server spawn --name local-skipped --config {
	remotes: { default: { url: $remote.url } },
	sync: { get: { checkout_pointers: false } },
}
tg --url $local_skipped.url pull $file
tg --url $local_skipped.url pull $large_file
let disabled_path = $local_skipped.checkout_directory | path join $file
let large_disabled_path = $local_skipped.checkout_directory | path join $large_file
assert (not ($disabled_path | path exists)) 'expected checkout pointers to be configurable'
assert (not ($large_disabled_path | path exists)) 'expected checkout pointers to be configurable'
assert equal (
	tg --url $local_skipped.url read $blob
	| str trim
) $contents 'expected disabled checkout pointers to retain leaf bytes in the store'

server stop $local_skipped
open $local_skipped.config_path
| upsert sync.get.checkout_pointers true
| to json
| save --force $local_skipped.config_path
let skipped = server start $local_skipped

tg --url $local_skipped.url pull $executable
let skipped_path = $local_skipped.checkout_directory | path join $executable
assert ($skipped_path | path exists) 'expected the skipped file to be materialized'
assert equal (open --raw $skipped_path) $contents

tg --url $local_skipped.url pull $eager_skipped --eager
let eager_skipped_path = $local_skipped.checkout_directory | path join $eager_skipped
assert equal (open --raw $eager_skipped_path) $contents

# Exercise batched existing-leaf loads and cached checkout-source handles across multiple batches.
tg --url $local_skipped.url pull $large_executable
let large_executable_path = $local_skipped.checkout_directory | path join $large_executable
assert equal (open --raw $large_executable_path | hash sha256) ($large_bytes | hash sha256)
tg --url $local_skipped.url pull $large_module --eager
let large_module_path = $local_skipped.checkout_directory | path join $large_module
assert equal (open --raw $large_module_path | hash sha256) ($large_bytes | hash sha256)

rm $skipped_path
rm $eager_skipped_path
rm $large_executable_path
rm $large_module_path
let output = tg --url $local_skipped.url read $blob | complete
failure $output 'expected the copied leaf bytes to be removed from the store'

# A directly pulled multi-leaf blob uses the same default file identity as tg write.
let local_branch = server spawn --name local-branch --config {
	remotes: { default: { url: $remote.url } },
}
let bytes = random binary 300000
let branch_blob = $bytes | tg --url $remote.url write --no-tokens | referent node
tg --url $local_branch.url pull $branch_blob
let entries = (
	ls $local_branch.checkout_directory
	| where { |entry| ($entry.name | path basename | str starts-with 'fil_') }
)
assert equal ($entries | length) 1
let branch_path = $entries.0.name
assert equal (open --raw $branch_path | hash sha256) ($bytes | hash sha256)

let local_writer = server spawn --name local-writer
let written_blob = $bytes | tg --url $local_writer.url write --no-tokens | referent node
assert equal $written_blob $branch_blob
let written_file = (
	ls $local_writer.checkout_directory
	| where { |entry| ($entry.name | path basename | str starts-with 'fil_') }
	| get 0.name
)
assert equal ($written_file | path basename) ($branch_path | path basename)

server stop $remote
rm $branch_path
let output = tg --url $local_branch.url get --bytes $branch_blob | complete
success $output 'expected the branch bytes to remain in the store'
let output = tg --url $local_branch.url read $branch_blob | complete
failure $output 'expected the multi-leaf blob to require its checkout pointer'
