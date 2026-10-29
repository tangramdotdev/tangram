use ../../lib/test.nu *
use ../../lib/vfs.nu

vfs skip_unless_supported

# A sandbox reads an entry of an input directory through the VFS while a sibling that it does not read is unavailable.
let source = server spawn --name source
let missing = tg --url $source.url put --no-tokens 'tg.file("missing\n")' | referent node
let local = server spawn --name local --config { vfs: true }
let directory = tg put --no-tokens ('tg.directory({ "missing": ' + $missing + ', "present": tg.file("hello\n") })') | referent node
let output = tg run --sandbox --executable /bin/sh --arg-string '-c' --arg-string 'IFS= read -r value < "/opt/tangram/store/$1/present"; test "$value" = hello' --arg-string sh --arg-value $directory | complete
success $output 'the sandbox should read the available entry'
