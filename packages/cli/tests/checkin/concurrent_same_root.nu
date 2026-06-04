use ../../test.nu *

# Concurrent checkins of distinct files under the same root all succeed.
#
# Regression test for 4245d307.

let server = spawn

let path = artifact {
	tangram.ts: 'export default "root";'
	a.txt: 'file a'
	b.txt: 'file b'
	c.txt: 'file c'
	d.txt: 'file d'
}

let outputs = [a.txt b.txt c.txt d.txt] | par-each --threads 4 { |file|
	tg checkin ($path | path join $file) | complete
}

for output in $outputs {
	success $output
}
