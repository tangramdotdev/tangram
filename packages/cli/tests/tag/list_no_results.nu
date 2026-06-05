use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg list --no-groups $pattern

snapshot -n output $output
