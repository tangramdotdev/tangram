use ../../test.nu *

# Archiving a directory that contains a file with dependencies fails instead of producing a truncated archive.

let server = spawn

let dir = tg put 'tg.directory({ "a": tg.file("first"), "z": tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "item": tg.file("d") } } }) })' | str trim

let tar_output = tg archive --format tar $dir | complete
failure $tar_output

let zip_output = tg archive --format zip $dir | complete
failure $zip_output
