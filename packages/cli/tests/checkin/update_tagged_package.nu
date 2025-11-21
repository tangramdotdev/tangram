use ../../test.nu *

let server = spawn

# Tag the old version of a.
let old_path = artifact {
	tangram.ts: '
		export default () => "a/1.0.0";
	'
}
run tg tag a/1.0.0 $old_path

# Create a package that depends on a/^1.
let local_path = artifact {
	tangram.ts: '
		import a from "a/^1";
		export default () => tg.run(a);
	'
}

# Check in the package.
run tg checkin $local_path

# Verify the lockfile has a/1.0.0.
let lockfile_path = $local_path | path join 'tangram.lock'
let lock = open $lockfile_path | from json
snapshot -n lock_before_update ($lock | to json -i 2)

# Tag a new version of a.
let new_path = artifact {
	tangram.ts: '
		export default () => "a/1.1.0";
	'
}
run tg tag a/1.1.0 $new_path

# Run update on the package.
run tg update $local_path

# Verify the lockfile has been updated to a/1.1.0.
let lock = open $lockfile_path | from json
snapshot -n lock_after_update ($lock | to json -i 2)
