use ../lib/test.nu *

# Extracting a blob that is not an archive fails.

let local = server spawn

let blob = "hello, world! this is not an archive at all, just text." | tg write --no-tokens | referent node

let output = tg extract $blob | complete
failure $output
