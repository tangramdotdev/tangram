use ../../test.nu *

# Touching an existing object succeeds.

let server = spawn

let id = tg put 'tg.file("touch me")' | str trim
let output = tg object touch $id | complete
success $output
