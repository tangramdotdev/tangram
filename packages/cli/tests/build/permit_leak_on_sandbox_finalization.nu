use ../../test.nu *

# A race between a process task's completion notification and its
# removal from the sandbox's `process_tasks` map can leave the sandbox
# task waiting forever: the sandbox task sees `process_tasks.is_empty()`
# return false, skips starting the ttl=0 timer, and never finalizes the
# sandbox. The race requires multi-threaded tokio.

let server = spawn -c {
	tokio_single_threaded: false,
	runner: { concurrency: 8 }
}

for round in 1..20 {
	1..20 | par-each --threads 20 { |i|
		let path = artifact {
			tangram.ts: $"export default \(\) => tg.file\(\"content-($round)-($i)\"\);"
		}
		tg build $path
	} | ignore
}

sleep 2sec

let db = ($server.directory | path join processes)
let stuck = ^sqlite3 $db "select count(*) from sandboxes where status = 'started';" | str trim | into int
assert ($stuck == 0) $"($stuck) sandboxes are stuck in 'started' after their only process finished"
