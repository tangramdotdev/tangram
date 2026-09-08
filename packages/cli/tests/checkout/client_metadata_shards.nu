use ../../test.nu *

# The client preserves complete dependency references across numbered shards and rejects incomplete or ambiguous metadata.

const repository_path = path self '../../../..'

let build = cargo build --quiet --manifest-path ($repository_path | path join Cargo.toml) --package tangram_client --example checkout_metadata | complete
success $build 'the checkout metadata example should build'
let reader = $repository_path | path join target debug examples checkout_metadata

let references = [
	'./dependency?location=remote&path=lib/injection#default'
	'./dependency?location=remote&path=lib/injection#default'
	'./unresolved'
]
let value = $references | to json --raw
let shards = $value | split chars | chunks 8 | each { str join }
assert (($shards | length) > 10) 'the fixture must distinguish numeric from lexical shard ordering'
let path = artifact 'wrapper'
for shard in ($shards | enumerate | reverse) {
	xattr_write $'user.tangram.dependencies.($shard.index)' $shard.item $path
}
let output = ^$reader $path | complete
success $output 'the client should reconstruct the complete dependency references'
let metadata = $output.stdout | from json
assert equal $metadata.dependencies $references
assert equal $metadata.token null

let incomplete = artifact 'wrapper'
xattr_write user.tangram.dependencies.1 '[]' $incomplete
failure (^$reader $incomplete | complete) 'a missing shard must not become an empty dependency list'

xattr_write user.tangram.dependencies '[]' $path
failure (^$reader $path | complete) 'mixed base and shard attributes must not discard either representation'

let malformed = artifact 'wrapper'
xattr_write user.tangram.dependencies '["./dependency"' $malformed
failure (^$reader $malformed | complete) 'a truncated dependency list must be reported'
