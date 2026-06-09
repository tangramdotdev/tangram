use ../../test.nu *

# Extracting a compressed blob that is not an archive fails, because a compression magic number makes the extractor assume a compressed tar archive.

let server = spawn

let blob = "hello, world! this is not an archive at all, just text." | tg write
let compressed = tg compress --format gz $blob | str trim

let output = tg extract $compressed | complete
failure $output
