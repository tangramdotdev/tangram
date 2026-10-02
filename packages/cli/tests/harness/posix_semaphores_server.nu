use ../lib/test.nu *

# A forcibly terminated server leaves locks which the test harness can remove.
const helper = path self '../../../../target/test/posix_semaphores'
let local = server spawn
let prefixes = [$local.config.index.posix_sem_prefix $local.config.cache.posix_sem_prefix]
let names = $prefixes | each {|prefix| [$'($prefix)r' $'($prefix)w'] } | flatten
success (^$helper --exists ...$names | complete)
let pid = open ($local.directory | path join lock) | into int
kill --signal 9 $pid
wait_until { ps | where pid == $pid | is-empty } 'the killed server should stop'
success (^$helper --exists ...$names | complete)
cleanup_posix_semaphores $env.TMPDIR
for name in $names {
	failure (^$helper --exists $name | complete)
}
