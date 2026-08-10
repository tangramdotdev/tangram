use ../../test.nu *

# A locked checkin rejects removing the last dependency instead of deleting the lockfile.

let server = spawn

let dependency_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $dependency_path

let path = artifact {
	tangram.ts: 'import a from "a/^1";'
}
tg checkin $path | ignore

let lockfile_path = $path | path join tangram.lock
let original_lock = open $lockfile_path

# Remove the only dependency, making the root no longer solvable.
'export default "no dependencies";' | save --force ($path | path join tangram.ts)

# --locked must reject the stale lock and leave it untouched.
let output = tg checkin $path --locked | complete
failure $output "removing the last dependency should make the lock out of date"
assert ($lockfile_path | path exists) "the locked checkin should not remove the lockfile"
assert ((open $lockfile_path) == $original_lock) "the locked checkin should not change the lockfile"
