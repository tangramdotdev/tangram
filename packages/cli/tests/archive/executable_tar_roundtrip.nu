use ../../test.nu *

# Archiving and extracting a directory with an executable file as tar preserves the executable bit.

let server = spawn

let dir = tg put 'tg.directory({ "tool": tg.file({ "contents": tg.blob("#!/bin/sh"), "executable": true }) })' | str trim

let blob = tg archive --format tar $dir | str trim
let extracted = tg extract $blob | str trim
assert equal $extracted $dir "the extracted directory should equal the original"
