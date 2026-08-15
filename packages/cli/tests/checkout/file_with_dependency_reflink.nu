use ../../test.nu *

# Checking out a file with dependencies writes the dependencies xattr, both when the file is uncached and when the server reflinks it from the checkouts directory.
#
# The reflink needs the checkouts directory and the destination on one filesystem which supports FICLONE, so this runs under the repository rather than TMPDIR, which is usually tmpfs.

const repository_path = path self '../../../..'

let root = $repository_path | path join 'target' | path join 'tests'
mkdir $root
let tmp = mktemp --directory --tmpdir-path $root

# Skip the test unless the filesystem supports reflinks.
let probe_path = $tmp | path join 'probe'
'probe' | save $probe_path
let probe = ^cp --reflink=always $probe_path ($tmp | path join 'probe_reflink') | complete
if $probe.exit_code != 0 {
	rm -rf $tmp
	skip_test 'this test requires a filesystem which supports reflinks'
}

let server = spawn --directory ($tmp | path join 'server')

let artifact = artifact {
	'bar.txt': 'bar',
	'mod.tg.ts': '
		import bar from "./bar.txt" with { type: "file" };
		export default bar;
	'
}

# Check the module in without checkout pointers, so that it is not checked out.
let id = tg checkin --no-checkout-pointers ($artifact | path join 'mod.tg.ts')

# The first checkout writes the file, because there is no internal checkout to reflink.
let cold_path = $tmp | path join 'cold'
tg checkout --dependencies=false $id --path $cold_path
assert ('user.tangram.dependencies' in (xattr_list $cold_path))

# Check out the file, then check it out again. This time the server reflinks it.
tg checkout $id
let warm_path = $tmp | path join 'warm'
tg checkout --dependencies=false $id --path $warm_path
assert ('user.tangram.dependencies' in (xattr_list $warm_path))

# Stop the server and unmount its sandbox before removing its custom directory.
cleanup_background_jobs $env.TMPDIR
rm -rf $tmp
