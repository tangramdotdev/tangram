use ../../test.nu *

# Disabling checkouts avoids creating or using the local checkout directory while preserving object I/O.

let server = server spawn --config {
	checkouts: false,
	roles: [http],
}
let store_path = $server.directory | path join store

assert (not ($store_path | path exists)) "expected the store directory to remain absent"

let blob = "hello" | tg write | str trim
assert equal (tg read $blob) "hello" "expected object I/O to work without checkouts"
assert (not ($store_path | path exists)) "expected object I/O not to create the store directory"

# Named-node mutations must not wait for an indexer when there are no physical checkouts to maintain.
tg tag dep $blob

let output = tg checkout dir_0000000000000000000000000000 | complete
failure $output
assert ($output.stderr | str contains "checkouts are disabled")
