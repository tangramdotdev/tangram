use ../../test.nu *

# A watched checkin with no intervening change is a pure cache hit that returns the same id as the first and as a cold checkin.

let server = spawn

let path = artifact {
	"a.txt": 'alpha'
}
let first = tg checkin $path --watch

# Check in again with no edit. The cache should be reused and the id unchanged.
let second = tg checkin $path --watch
assert ($first == $second) "a watched checkin with no change should return the same id"

# A cold checkin ignores the watch cache and should still agree.
let cold = tg checkin $path
assert ($first == $cold) "the watched id should equal a cold checkin"
