use ../../lib/test.nu *

# A forcibly terminated server leaves locks which the test harness can remove.
let local = server spawn
let prefixes = [$local.config.index.posix_sem_prefix $local.config.cache.posix_sem_prefix]
let names = $prefixes | each {|prefix| [$'($prefix)r' $'($prefix)w'] } | flatten
success (tangram_tests_runner semaphore exists ...$names | complete)
let pid = open ($local.directory | path join lock) | into int
kill --signal 9 $pid
wait_for_exit $pid 'the killed server should stop'
success (tangram_tests_runner semaphore exists ...$names | complete)
cleanup_posix_semaphores $env.TMPDIR
for name in $names {
	failure (tangram_tests_runner semaphore exists $name | complete)
}
