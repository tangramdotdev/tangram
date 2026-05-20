#!/usr/bin/env nu

# Builds the flex VFS artifact (/tmp/flex-vfs.tg.ts) and reads it back through
# the mounted FSKit VFS at ~/.tangram/artifacts/<id> to confirm the read path
# works for every operation we care about: lookup, getattr, readdir, read, and
# readlink. This lives outside the test suite because the FSKit VFS is a global,
# machine-wide resource the harness cannot isolate.

def main [] {
	let source = "/tmp/flex-vfs.tg.ts"

	print $"building ($source)"
	let id = (^tangram build $source | str trim)
	if ($id | is-empty) or (not ($id | str starts-with "dir_")) {
		print --stderr $"unexpected build output: ($id)"
		exit 1
	}
	print $"artifact: ($id)"

	let root = ($env.HOME | path join ".tangram/artifacts" $id)
	print $"vfs path: ($root)\n"

	print "=== tree ==="
	^tree -a $root

	print "\n=== file contents (read) ==="
	for file in ["hello.txt" "empty.txt" "nested/a.txt" "nested/deep/c.txt" "inner/leaf.txt" "inner/nested/deep.txt"] {
		let path = ($root | path join $file)
		print $"--- ($file) ---"
		^cat $path
	}

	print "\n=== executable bit (getattr) ==="
	^ls -l ($root | path join "run.sh")

	print "\n=== symlink targets (readlink) ==="
	for link in ["link-file" "link-nested" "link-dir" "link-artifact" "link-artifact-subpath"] {
		let path = ($root | path join $link)
		print $"($link) -> (^readlink $path)"
	}

	print "\n=== read through symlinks ==="
	print $"link-file: (^cat ($root | path join 'link-file'))"
	print $"link-nested: (^cat ($root | path join 'link-nested'))"
	print $"link-artifact-subpath: (^cat ($root | path join 'link-artifact-subpath'))"

	print "\ndone"
}
