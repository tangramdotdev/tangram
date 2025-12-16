use ../../test.nu *

# This test verifies that many concurrent checkins of the same root all succeed.

let server = spawn

let path = artifact {
	tangram.ts: 'export default 0;'
}

for iteration in 1..10 {
	let outputs = 1..10 | par-each --threads 50 { |i|
		tg checkin $path | complete
	}
	for output in $outputs {
		success $output
	}
}
