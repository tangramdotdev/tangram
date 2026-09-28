use ../lib/test.nu *

# Formatting an already formatted module is a no-op.

let local = server spawn

let dir = mktemp --directory
'export default   "x"' | save ($dir | path join tangram.ts)

tg format $dir
let once = open ($dir | path join tangram.ts)
tg format $dir
let twice = open ($dir | path join tangram.ts)
assert equal $once $twice "the second format should not change the module"
