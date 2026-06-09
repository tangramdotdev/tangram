use ../../test.nu *

# Extracting a blob that is not an archive fails.

let server = spawn

let blob = "hello, world! this is not an archive at all, just text." | tg write

let output = tg extract $blob | complete
failure $output
