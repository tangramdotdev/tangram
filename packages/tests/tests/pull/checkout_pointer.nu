use ../../lib/test.nu *

# Pulled files and blobs remain readable after the remote stops, whether checkout pointers are enabled, disabled, or enabled after earlier pulls.

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

# Pull lazily and eagerly with checkout pointers enabled.
tg pull $file
tg pull $eager_file --eager

# Pull with checkout pointers disabled, then pull more files after enabling them.
let local_skipped = server spawn --name local-skipped --config {
	remotes: { default: { url: $remote.url } },
	sync: { get: { checkout_pointers: false } },
}
tg --url $local_skipped.url pull $file
tg --url $local_skipped.url pull $large_file
server stop $local_skipped
open $local_skipped.config_path
| upsert sync.get.checkout_pointers true
| to json
| save --force $local_skipped.config_path
let local_skipped = server start $local_skipped
tg --url $local_skipped.url pull $executable
tg --url $local_skipped.url pull $eager_skipped --eager
tg --url $local_skipped.url pull $large_executable
tg --url $local_skipped.url pull $large_module --eager

# Pull a multi-leaf blob directly.
let local_branch = server spawn --name local-branch --config {
	remotes: { default: { url: $remote.url } },
}
let bytes = random binary 300000
let branch_blob = $bytes | tg --url $remote.url write --no-tokens | referent node
tg --url $local_branch.url pull $branch_blob

# Every pulled file and blob is readable without the remote.
server stop $remote
assert equal (tg --url $local.url read $file) $contents
assert equal (tg --url $local.url read $blob) $contents
assert equal (tg --url $local.url read $eager_file) 'eager checkout pointer'
assert equal (tg --url $local_skipped.url read $file) $contents
assert equal (tg --url $local_skipped.url read $large_file | hash sha256) ($large_bytes | hash sha256)
assert equal (tg --url $local_skipped.url read $executable) $contents
assert equal (tg --url $local_skipped.url read $eager_skipped) $contents
assert equal (tg --url $local_skipped.url read $large_executable | hash sha256) ($large_bytes | hash sha256)
assert equal (tg --url $local_skipped.url read $large_module | hash sha256) ($large_bytes | hash sha256)
assert equal (tg --url $local_branch.url read $branch_blob | hash sha256) ($bytes | hash sha256)
