use ../../util/test.nu *

# Omitting run preserves module selection, run options, globals, and trailing arguments.

let local = server spawn
let path = artifact {
	tangram.ts: '
		export default (...args: Array<tg.Value>) => args;
		export const install = (...args: Array<tg.Value>) => args;
	'
}
let reference = $'($path)#install'
let output = tg --url $local.url $reference -a first -A true -a second -- 'two words' --help '' | from json
assert equal $output [first true second 'two words' --help '']
let output = do { cd $path; tg .#install -- local } | from json
assert equal $output [local]

# References to already checked-in packages and tags also use the shorthand.
let id = tg checkin $path | referent node
let output = tg $id -- artifact | from json
assert equal $output [artifact]
tg tag put implicit-run $id
let output = tg implicit-run -- tagged | from json
assert equal $output [tagged]

# Built-ins keep precedence over tags of the same name.
tg tag put health $id
let output = tg health --fields version | from json
assert ('version' in $output)
let output = tg run health -- explicit | from json
assert equal $output [explicit]
