use ../lib/test.nu *

# Reverting a watched file back to its original contents restores the original id, since checkin is purely content addressed.

let local = server spawn

let path = artifact {
	"a.txt": 'one'
}
let first = tg checkin --no-tokens $path --watch | referent node

# Edit the file and check in.
'two' | save --force ($path | path join 'a.txt')
tg watch touch $path ($path | path join 'a.txt')
let edited = tg checkin --no-tokens $path --watch | referent node
assert ($first != $edited) "editing the file should change the id"

# Revert the file to its original contents and check in.
'one' | save --force ($path | path join 'a.txt')
tg watch touch $path ($path | path join 'a.txt')
let reverted = tg checkin --no-tokens $path --watch | referent node
assert ($first == $reverted) "reverting the contents should restore the original id"
