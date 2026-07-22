use ../../test.nu *

# Checking out a file with dependencies writes the dependencies xattr, both when the file is uncached and when the server reflinks it from the cache.
#
# The reflink needs the cache and the destination on one filesystem which supports FICLONE, so this runs under the repository rather than TMPDIR, which is usually tmpfs.

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

# Check the module in without cache pointers, so that it is not cached.
let id = tg checkin --no-cache-pointers ($artifact | path join 'mod.tg.ts')

# The first checkout writes the file, because there is no cache entry to reflink.
let cold_path = $tmp | path join 'cold'
tg checkout --dependencies=false $id $cold_path
assert ('user.tangram.dependencies' in (xattr_list $cold_path))

# Cache the file, then check it out again. This time the server reflinks it.
tg cache $id
let warm_path = $tmp | path join 'warm'
tg checkout --dependencies=false $id $warm_path
assert ('user.tangram.dependencies' in (xattr_list $warm_path))

rm -rf $tmp
