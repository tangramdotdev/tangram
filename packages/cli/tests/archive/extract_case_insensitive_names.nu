use ../../test.nu *

# Extracting a tar archive containing two entries whose names differ only in case produces a directory with both entries on a case sensitive file system, and reports the case conflict on a case insensitive one. With `--overwrite` the conflict is not an error, and the last entry wins on either file system.

# Create the entries in separate directories, because a case insensitive file system cannot hold both at once.
let lower = artifact {
	'chap.html': 'lower'
}
let upper = artifact {
	'Chap.html': 'upper'
}

# Create a tar archive containing both entries.
let archive = mktemp -d | path join 'case.tar'
^tar -cf $archive -C $lower 'chap.html'
^tar -rf $archive -C $upper 'Chap.html'

# Determine whether the file system is case sensitive.
let probe = mktemp -d
'probe' | save -f ($probe | path join 'probe')
let case_sensitive = not (($probe | path join 'PROBE') | path exists)

# Extract the archive.
let extracted = mktemp -d | path join 'output'
let output = tg builtin extract --input $archive --output $extracted | complete

if $case_sensitive {
	# The extracted directory should contain both entries.
	success $output "the archive should extract"
	assert (($extracted | path join 'chap.html') | path exists) "the extracted directory should contain the lowercase entry"
	assert (($extracted | path join 'Chap.html') | path exists) "the extracted directory should contain the capitalized entry"
} else {
	# The extraction should report the case conflict rather than a bare "file exists" error.
	failure $output "the archive should not extract"
	assert ($output.stderr | str contains 'failed to extract the archive entry') "the error should identify the failing archive entry"
	assert ($output.stderr | str contains 'differs only in case') "the error should explain the case conflict"
	assert ($output.stderr | str contains 'chap.html') "the error should name the conflicting entry"
}

# Extract the archive with `--overwrite`. The capitalized entry comes last, so it wins on a case sensitive file system by name and on a case insensitive one by replacing the lowercase entry.
let overwritten = mktemp -d | path join 'overwritten'
let output = tg builtin extract --input $archive --overwrite --output $overwritten | complete
success $output "the archive should extract"
snapshot (open --raw ($overwritten | path join 'Chap.html')) 'upper'
