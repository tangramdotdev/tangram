use ../lib/test.nu *

# An exact subtree token authorizes spawning a command that the user reaches only through that token, alongside a command the user reaches through a grant.

let root = random chars
let local = server spawn --config {
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	verification: {
		index: { delay: null }
	}
}
let bob = tg login --verbose --name bob | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let host = $'((^uname -m | str trim | str replace arm64 aarch64))-((^uname -s | str trim | str lowercase))'

def command-id [root: string, host: string, executable: string] {
	let command = { executable: $executable, host: $host } | to json --raw
	tg --token $root put --no-tokens $"tg.command\(($command))" | referent node
}

# Bob reaches one command through an exact token and the other through a grant.
let proven = command-id $root $host '/bin/true'
let granted = command-id $root $host '/bin/false'
tg --token $root grant $bob.user.id object_subtree $granted | ignore
tg --token $root index

let token = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root)' } --unix-socket $socket $'http://localhost/objects/($proven)'
	| get tokens.local.0
let headers = { Authorization: $'Bearer ($bob.token)', 'Content-Type': 'application/json' }
let denied = http get --allow-errors --full --headers $headers --unix-socket $socket $'http://localhost/objects/($proven)'
assert equal $denied.status 404 'Bob must not reach the command without the token'

def spawn [socket: string, headers: record, command: record] {
	let arg = { command: $command, sandbox: {}, stderr: 'null', stdin: 'null', stdout: 'null' } | to json --raw
	let response = http post --raw --max-time 10sec --headers $headers --unix-socket $socket 'http://localhost/processes/spawn' $arg
	let output = $response | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
	assert ($output.process? != null) $'the spawn must be authorized: ($response)'
}

spawn $socket $headers { node: $proven, options: { tokens: { local: [$token] } } }
spawn $socket $headers { node: $granted, options: {} }
