use ../../test.nu *

# The client distinguishes an ordinary file without checkout metadata from an explicit empty dependency list or unreadable metadata.

const repository_path = path self '../../../..'

let build = cargo build --quiet --manifest-path ($repository_path | path join Cargo.toml) --package tangram_client --example checkout_metadata | complete
success $build 'the checkout metadata example should build'
let reader = $repository_path | path join target debug examples checkout_metadata

let path = artifact 'ordinary executable'
let output = ^$reader $path | complete
success $output 'an ordinary executable should have absent checkout metadata'
assert equal ($output.stdout | from json) { dependencies: null, token: null }

xattr_write user.tangram.dependencies '[]' $path
let output = ^$reader $path | complete
success $output 'an explicit empty dependency list should be preserved'
assert equal ($output.stdout | from json) { dependencies: [], token: null }

xattr_write user.tangram.token invalid $path
failure (^$reader $path | complete) 'a malformed token must not be silently dropped'
failure (^$reader ($path + '.missing') | complete) 'a missing file must be reported'
failure (^$reader ($path | path dirname) | complete) 'a directory must not be treated as a checked-out file'
