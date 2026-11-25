use ../../util/test.nu *

# A checkin with checkout pointers produces the same artifact and checkout whether it copies each file with a reflink or with a stream.

let contents = 1..20000 | each { |line| $'line ($line)' } | str join "\n"
let path = artifact {
	directory: {
		executable: (file --executable 'echo hello')
		large: $contents
		nested: {
			small: 'small'
		}
		link: (symlink 'nested/small')
	}
}
let path = $path | path join directory

# Check in the directory with each copy mode on its own server.
let reflink = server spawn --name reflink --config { checkin: { copy: 'reflink' } }
let reflink_id = tg --url $reflink.url checkin --no-tokens --checkout-pointers $path | referent node
let stream = server spawn --name stream --config { checkin: { copy: 'stream' } }
let stream_id = tg --url $stream.url checkin --no-tokens --checkout-pointers $path | referent node

# The artifacts are the same, and the streamed checkin's checkout has the files' contents and modes.
assert equal $stream_id $reflink_id
let checkout = mktemp --directory | path join checkout
tg --url $stream.url checkout $stream_id --path $checkout
assert equal (open --raw ($checkout | path join executable)) 'echo hello'
assert (ls -l ($checkout | path join executable) | get 0.mode | str contains 'x')
assert equal (open --raw ($checkout | path join large)) $contents
assert equal (open --raw ($checkout | path join nested small)) 'small'
assert equal (ls -l ($checkout | path join link) | get 0.target) 'nested/small'
