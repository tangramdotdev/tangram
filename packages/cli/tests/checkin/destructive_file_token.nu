use ../../test.nu *

# Compare identical files in separate stores so neither checkin reuses the other checkout.
let ordinary = server spawn --config { vfs: false }
let ordinary_id = tg checkin --no-ignore (artifact 'hello') | str trim
let checkout = $ordinary.checkout_directory | path join $ordinary_id
assert ('user.tangram.token' in (xattr_list $checkout))
server stop $ordinary

# Build output collection uses destructive checkin, which must also write the file token.
let destructive = server spawn --config { vfs: false }
let destructive_id = tg checkin --destructive --no-ignore (artifact 'hello') | str trim
assert equal $destructive_id $ordinary_id
let checkout = $destructive.checkout_directory | path join $destructive_id
assert ('user.tangram.token' in (xattr_list $checkout)) 'destructive checkin must write the file token'

# Reusing the cached file must retain its token.
assert equal (tg checkout $destructive_id | str trim) $checkout
assert ('user.tangram.token' in (xattr_list $checkout))
server stop $destructive
