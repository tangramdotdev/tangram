use ../../test.nu *

# Reverting a watched file back to its original contents restores the original id, since checkin is purely content addressed.

let server = spawn

let path = artifact {
	"a.txt": 'one'
}
let first = tg checkin $path --watch

# Edit the file and check in.
'two' | save --force ($path | path join 'a.txt')
tg watch touch $path ($path | path join 'a.txt')
let edited = tg checkin $path --watch
assert ($first != $edited) "editing the file should change the id"

# Revert the file to its original contents and check in.
'one' | save --force ($path | path join 'a.txt')
tg watch touch $path ($path | path join 'a.txt')
let reverted = tg checkin $path --watch
assert ($first == $reverted) "reverting the contents should restore the original id"
