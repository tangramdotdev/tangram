use ../lib/test.nu *

# Archiving a directory that contains a file with dependencies fails instead of producing a truncated archive.

let local = server spawn

let dir = tg put --no-tokens 'tg.directory({ "a": tg.file("first"), "z": tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "node": tg.file("d") } } }) })' | referent node

let tar_output = tg archive --format tar $dir | complete
failure $tar_output

let zip_output = tg archive --format zip $dir | complete
failure $zip_output
