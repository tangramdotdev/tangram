use ../../util/test.nu *

# A watched source can be checked in again after cleaning its objects and checkout.

let local = server spawn
let path = artifact { file: 'hello' }
let before = tg checkin --no-tokens $path | referent node
tg clean | ignore

let after = tg checkin --no-tokens $path | referent node
assert equal $after $before
assert equal (tg read $'($after)?get=file') 'hello'
success (tg watch touch $path ($path | path join file) | complete) 'the watch should remain usable after cleaning'
