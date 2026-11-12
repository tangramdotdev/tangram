use ../../util/test.nu *

# Checking in a directory inside an ignored directory of an enclosing package does not read the rest of the enclosing package.

let local = server spawn

let path = artifact {
	.gitignore: '/ignored'
	ignored: {
		package: {
			tangram.ts: ''
			tree: {
				file.txt: 'hello'
			}
		}
		tree: {
			file.txt: 'hello'
		}
	}
	other: {
		file.txt: 'other'
	}
	tangram.ts: ''
}

# Make a directory of the enclosing package unreadable, so that a checkin which reads it fails.
let other = $path | path join 'other'
chmod 000 $other
let directory = tg checkin --no-tokens ($path | path join 'ignored' 'tree') | complete
let nested = tg checkin --no-tokens ($path | path join 'ignored' 'package' 'tree') | complete
chmod 755 $other
success $directory "the directory checkin should not read the enclosing package"
success $nested "the checkin inside a nested package should not read the enclosing package"
