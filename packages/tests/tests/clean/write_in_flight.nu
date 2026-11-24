use ../../util/test.nu *

# A write that is in flight while the server cleans completes successfully.

let server = server spawn --config { advanced: { checkpoints: true } }

# Hold a write once it has written its blob, and clean while it is held.
let written = checkpoint watch write.written
let write = job spawn {
	let id = job id
	'hello, world!' | tg write --no-tokens | complete | job send --tag $id 0
}
checkpoint wait $written | ignore
success (tg clean | complete) 'the clean should succeed'
checkpoint continue $written
checkpoint unwatch $written

# The write completes, and its blob has its contents.
let output = job recv --tag $write --timeout 60sec
success $output 'the write should succeed'
let blob = $output.stdout | referent node
assert equal (tg read $blob) 'hello, world!'
