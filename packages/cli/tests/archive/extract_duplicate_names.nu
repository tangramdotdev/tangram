use ../../test.nu *

# Extracting a tar archive that contains the same entry twice is rejected, unless `--overwrite` is set, in which case the last entry wins, as tar does.

# Create the entries in separate directories, because a directory cannot hold both at once.
let first = artifact {
	'greeting': 'first'
}
let second = artifact {
	'greeting': 'second'
}

# Create a tar archive containing the entry twice.
let archive = mktemp -d | path join 'duplicate.tar'
^tar -cf $archive -C $first 'greeting'
^tar -rf $archive -C $second 'greeting'

# Without `--overwrite`, the repeated entry should be rejected.
let rejected = mktemp -d | path join 'rejected'
let output = tg builtin extract --input $archive --output $rejected | complete
failure $output "the archive should not extract"
assert ($output.stderr | str contains 'failed to create the file') "the error should identify the failing archive entry"

# With `--overwrite`, the archive should extract and the last entry should win.
let extracted = mktemp -d | path join 'extracted'
let output = tg builtin extract --input $archive --overwrite --output $extracted | complete
success $output "the archive should extract"
assert ((open --raw ($extracted | path join 'greeting')) == 'second') "the last entry should replace the first"
