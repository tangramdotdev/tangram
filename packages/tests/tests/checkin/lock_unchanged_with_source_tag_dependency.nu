use ../../util/test.nu *

# Checking in a package again leaves its unchanged lockfile in place when a tag dependency has a source import.

let local = server spawn

let path = artifact {
	dependencies: {
		b.tg.ts: ''
		a.tg.ts: '
			import "b" with { source: "./b.tg.ts" };
		'
	}
	package: {
		tangram.ts: '
			import "a";
		'
	}
}
tg tag b ($path | path join dependencies b.tg.ts)
tg tag a ($path | path join dependencies a.tg.ts)

let package_path = $path | path join package
let lockfile_path = $package_path | path join tangram.lock
tg checkin --no-tokens --no-watch $package_path | ignore
let before = ls --long $lockfile_path | first | get inode

tg checkin --no-tokens --no-watch $package_path | ignore
let after = ls --long $lockfile_path | first | get inode
assert equal $after $before "the checkin should not rewrite the unchanged lockfile"
