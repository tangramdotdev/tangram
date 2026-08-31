use ../../test.nu *

# Authorizing a directly granted resource should not get more expensive as the principal gains descendants.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	tracing: {
		filter: 'tangram=info,tangram_index::authorize=debug'
		stderr_format: 'json'
	}
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let groups = 0..<80 | each {|index|
	tg --token $alice.token group create $'group-($index)' | from json
}
let target = $groups | sort-by id | last
let others = $groups | where id != $target.id

tg --token $alice.token grant $bob.user.id read $target.id | ignore
tg --token $alice.token index
for _ in 0..<8 {
	tg --token $bob.token group get $target.id | ignore
}

for group in $others {
	tg --token $alice.token grant $bob.user.id read $group.id | ignore
}
tg --token $alice.token index
for _ in 0..<8 {
	tg --token $bob.token group get $target.id | ignore
}

let reads = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { from json }
	| where $it.fields.message? == 'authorize batch'
	| where $it.fields.args? == 1
	| where $it.fields.resource? == $target.id
	| get fields.reads
	| last 16
assert (($reads | length) == 16) 'expected sixteen authorization measurements'

let first = $reads | first 8 | math avg
let last = $reads | last 8 | math avg
print $'authorization reads grew from an average of ($first) to ($last)'
assert ($last <= $first) 'authorizing the same group got more expensive as the principal gained descendants'
