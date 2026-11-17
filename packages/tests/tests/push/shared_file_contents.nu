use ../../util/test.nu *

# Eagerly pushing a directory in which pairs of files share their contents succeeds, and the remote serves both files of a pair.

let remote = server spawn --name remote
let local = server spawn --name local
tg remote put default $remote.url

# Create many pairs of distinct files with shared contents to exercise copies while writes are still pending.
let path = mktemp -d
for index in 0..<1500 {
	let bytes = random binary 120000
	$bytes | save ($path | path join $'plain_($index)')
	$bytes | save ($path | path join $'executable_($index)')
}
chmod +x ...(glob ($path | path join 'executable_*'))
let id = tg checkin --no-tokens $path | referent node

# Push the directory.
let output = tg push $id --eager | complete
success $output 'the push should succeed when files share their contents'

# Read both files of a pair through the pushed directory.
let expected = open --raw ($path | path join 'plain_0') | hash sha256
for name in ['plain_0' 'executable_0'] {
	assert equal (tg --url $remote.url read $'($id)?get=($name)' | hash sha256) $expected $'the remote should serve ($name)'
}
