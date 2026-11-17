use ../../util/test.nu *

# Eagerly pushing a directory in which pairs of files share their contents succeeds, and the remote serves both files of a pair.

let remote = server spawn --name remote
let local = server spawn --name local
tg remote put default $remote.url

# Create pairs of files with the same contents, where only one file of each pair is executable.
let path = mktemp -d | path join 'artifact'
mkdir $path
let count = 1500
for index in 0..<$count {
	let bytes = random binary 120000
	$bytes | save ($path | path join $'plain_($index)')
	$bytes | save ($path | path join $'executable_($index)')
}
chmod +x ...(glob ($path | path join 'executable_*'))
let id = tg checkin --no-tokens $path | referent node

# Push the directory.
let output = tg push $id --eager | complete
success $output 'the push should succeed when files share their contents'

# Both files of a pair are readable on the remote.
let expected = open --raw ($path | path join 'plain_0') | hash sha256
for name in ['plain_0' 'executable_0'] {
	let file = tg checkin --no-tokens ($path | path join $name) | referent node
	assert equal (tg --url $remote.url read $file | hash sha256) $expected $'the remote should serve ($name)'
}
