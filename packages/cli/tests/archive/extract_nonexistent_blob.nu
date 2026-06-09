use ../../test.nu *

# Extracting a well formed blob id that does not exist fails.

let server = spawn

let output = tg extract blb_010000000000000000000000000000000000000000000000000000 | complete
failure $output
