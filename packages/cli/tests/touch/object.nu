use ../lib/test.nu *

# Touching an existing object succeeds.

let local = server spawn

let id = tg put --no-tokens 'tg.file("touch me")' | referent node
let output = tg object touch $id | complete
success $output
