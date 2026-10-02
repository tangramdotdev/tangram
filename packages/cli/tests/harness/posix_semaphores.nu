use ../lib/test.nu *

# Cleanup removes orphaned test semaphores without touching another environment.
const helper = path self '../../../../target/test/posix_semaphores'
let prefix = $'/tgt-((random chars) | str lowercase | str substring 0..15)'
let names = [$'($prefix)r' $'($prefix)w']
let unrelated = $'($prefix)x'
let directory = mktemp -d
(($names | str join "\n") + "\n") | save ($directory | path join 'posix_semaphores')
([$names $unrelated] | flatten | str join "\n") ++ "\n" | save --append ($env.TMPDIR | path join 'posix_semaphores')

# The creator exits without unlinking, as a crashed server would.
success (^$helper --create ...$names $unrelated | complete)
success (^$helper --exists ...$names $unrelated | complete)
cleanup_posix_semaphores $directory
for name in $names {
	let output = ^$helper --exists $name | complete
	failure $output
	assert ($output.stderr | str contains 'No such file or directory')
}
success (^$helper --exists $unrelated | complete)

# Repeated cleanup succeeds when graceful shutdown already removed the names.
cleanup_posix_semaphores $directory
success (^$helper $unrelated | complete)
