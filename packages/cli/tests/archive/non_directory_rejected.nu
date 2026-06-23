use ../../test.nu *

# Archiving an artifact that is not a directory fails instead of producing an unusable archive.

let server = spawn

let file_id = tg put 'tg.file("contents")' | str trim

let tar_output = tg archive --format tar $file_id | complete
failure $tar_output

let zip_output = tg archive --format zip $file_id | complete
failure $zip_output
