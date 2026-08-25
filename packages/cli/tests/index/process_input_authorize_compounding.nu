use ../../test.nu *

# A process reading its own input should not get slower as other packages accumulate parents on that input.

let server = server spawn

# The file the reader reads. It never changes.
let shared = tg checkin (artifact 'shared') | str trim

# A process that reads the shared file, the way a linker reads its inputs. It reads in a loop because one read is a couple of milliseconds, too small to see next to the cost of starting the process.
let reader = artifact {
	tangram.ts: '
		export default async function (input: tg.File, salt: string) {
			for (let i = 0; i < 200; i++) {
				await tg.File.withId(input.id).load();
			}
		}
	',
}

# Time one run of the reader. The salt makes it a new process, so its reads are not served from the cache.
def read_secs [reader: path, shared: string, salt: string] {
	let start = (date now)
	tg build $reader --arg-value $shared --arg-string $salt
	((date now) - $start) / 1sec
}

let cold = (read_secs $reader $shared 'cold')

# Give the shared file 640 parents, the way 640 other packages depending on it would.
mut entries = {}
for index in 0..<640 {
	$entries = ($entries | insert $"w($index)" { shared: 'shared', salt: $"($index)" })
}
tg checkin (artifact $entries) | ignore
tg index

let warm = (read_secs $reader $shared 'warm')

let ratio = ($warm / $cold | math round -p 2)
print $"cold=($cold | math round -p 2)s warm=($warm | math round -p 2)s ratio=($ratio)x"
assert ($ratio < 2.5) $"reading an unchanged input cost ($ratio)x more once it had 640 unrelated parents"
