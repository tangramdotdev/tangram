use ../../test.nu *

# Extracting a tar archive that contains the same entry twice reports the repeated entry, and with `--overwrite` it succeeds and the last entry replaces the first, as tar does.

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

# Extract the archive.
let extracted = mktemp -d | path join 'output'
let output = tg builtin extract --input $archive --output $extracted | complete
failure $output "the archive should not extract"

# Drop the progress frames, whose count and byte counts vary from run to run.
let error = $output.stderr | lines | where { |line| not ($line | str starts-with 'extracting') }
snapshot --redact $extracted $error '
	-> failed to create the file
	   path = "<redacted>/greeting"
	-> File exists (os error 17)

'

# Extract the archive with `--overwrite`.
let overwritten = mktemp -d | path join 'overwritten'
let output = tg builtin extract --input $archive --overwrite --output $overwritten | complete
success $output "the archive should extract"
snapshot (open --raw ($overwritten | path join 'greeting')) 'second'
