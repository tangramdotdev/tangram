use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg tag list $pattern

snapshot -n output $output
