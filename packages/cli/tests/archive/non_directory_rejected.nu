use ../lib/test.nu *

# Archiving an artifact that is not a directory fails instead of producing an unusable archive.

let local = server spawn

let file_id = tg put --no-tokens 'tg.file("contents")' | referent node

let tar_output = tg archive --format tar $file_id | complete
failure $tar_output

let zip_output = tg archive --format zip $file_id | complete
failure $zip_output
