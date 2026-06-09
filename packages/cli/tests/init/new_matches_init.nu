use ../../test.nu *

# tg new produces the same scaffold as tg init.

let init_dir = mktemp --directory
tg init $init_dir

let new_dir = mktemp --directory
let output = tg new $new_dir | complete
success $output

assert equal (open ($new_dir | path join tangram.ts)) (open ($init_dir | path join tangram.ts)) "the scaffolds should be identical"
