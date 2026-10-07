use ../lib/test.nu *

let local = server spawn
let first = artifact {tangram.ts: 'export const value = 1;'}
tg tag -p dependency/1.0.0 $first
let path = artifact {tangram.ts: 'import { value } from "dependency/^1";'}
let original = tg checkin --no-tokens $path --watch --locked

let second = artifact {tangram.ts: 'export const value = 2;'}
tg tag -p dependency/1.1.0 $second
tg index
let second_id = tg tag get dependency/1.1.0 | from json | get target.id

# Change only the lockfile and deliver its event before the next checkin.
let lockfile_path = $path | path join tangram.lock
let lock = open --raw $lockfile_path | from json
let nodes = $lock.nodes | each { |node|
	if $node.kind == 'file' {
		$node | update dependencies {
			'dependency/^1': {
				item: null
				options: {id: $second_id, tag: dependency/1.1.0}
			}
		}
	} else {
		$node
	}
}
$lock | update nodes $nodes | to json | save --force $lockfile_path
tg watch touch $path $lockfile_path

# The watched graph must match a fresh checkin of the updated lockfile.
let updated = tg checkin --no-tokens $path --watch --locked
let fresh = tg checkin --no-tokens $path --locked
assert not equal $updated $original
assert equal $updated $fresh
