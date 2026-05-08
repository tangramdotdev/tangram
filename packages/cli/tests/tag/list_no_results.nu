use ../../test.nu *

let server = spawn

let pattern = "test"
let output = tg list --no-namespaces $pattern

snapshot -n output $output
