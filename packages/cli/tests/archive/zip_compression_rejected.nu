use ../../test.nu *

# Requesting compression with the zip format fails, because zip archives have their own internal compression.

let server = server spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let output = tg archive --format zip --compression gz $dir | complete
failure $output
