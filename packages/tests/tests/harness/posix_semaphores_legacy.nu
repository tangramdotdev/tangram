use ../../util/test.nu *

# Recover an older index's default names from its surviving lock file.
let local = server spawn
server stop $local
open $local.config_path | reject index.posix_sem_prefix | save -f $local.config_path
let local = server start $local
let lockfile = $local.directory | path join 'index.lmdb-lock'
let names = tangram_tests_runner semaphore names $lockfile | lines
assert equal ($names | length) 2
success (tangram_tests_runner semaphore exists ...$names | complete)
let pid = open ($local.directory | path join lock) | into int
kill --signal 9 $pid
wait_for_exit $pid 'the killed server should stop'
success (tangram_tests_runner semaphore exists ...$names | complete)
cleanup_posix_semaphores $env.TMPDIR
for name in $names {
	failure (tangram_tests_runner semaphore exists $name | complete)
}
