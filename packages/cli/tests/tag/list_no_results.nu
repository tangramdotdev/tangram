use ../../test.nu *

# tg list produces empty output when no tag matches the requested pattern.

let server = spawn

let pattern = "test"
let output = tg list --no-groups $pattern

snapshot --name output $output
