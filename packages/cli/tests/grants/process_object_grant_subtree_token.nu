use ../lib/test.nu *

# An exact subtree token must authorize a process command grant without another authorization search.

let root = random chars
let server = server spawn --preserve-keys --config {
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
	authorization: { index: { delay: null } }
	tracing: { filter: 'tangram=info,tangram_index::authorize::engine=debug', stderr_format: json }
}
let bob = tg login --verbose --name bob | from json
let socket = $server.url | str replace 'http+unix://' '' | url decode
let host = $'((^uname -m | str trim | str replace arm64 aarch64))-((^uname -s | str trim | str lowercase))'

def command-id [root: string, host: string, executable: string] {
	let command = { executable: $executable, host: $host } | to json --raw
	tg --token $root put $"tg.command\(($command))" | str trim
}

# Bob reaches one command through an exact token and the other through a grant.
let proven = command-id $root $host '/bin/true'
let granted = command-id $root $host '/bin/false'
tg --token $root grant $bob.user.id object_subtree $granted | ignore
tg --token $root index

let token = http get --headers { Accept: 'application/json', Authorization: $'Bearer ($root)' } --unix-socket $socket $'http://localhost/objects/($proven)'
	| get tokens.local.authorization.0
let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
assert equal $body.resource $proven
assert ('object_subtree' in $body.permissions) 'the token must prove subtree access to the command'
let headers = { Authorization: $'Bearer ($bob.token)', 'Content-Type': 'application/json' }
let denied = http get --allow-errors --full --headers $headers --unix-socket $socket $'http://localhost/objects/($proven)'
assert equal $denied.status 404 'Bob must not reach the command without the token'

# Restart to exclude the setup's searches, which requires preserved keys so the token still verifies.
server stop $server
let offset = open --raw $server.log | lines | length
let server = server start $server

def spawn [socket: string, headers: record, command: record] {
	let arg = { command: $command, sandbox: {}, stderr: 'null', stdin: 'null', stdout: 'null' } | to json --raw
	let response = http post --raw --max-time 10sec --headers $headers --unix-socket $socket 'http://localhost/processes/spawn' $arg
	let output = $response | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
	assert ($output.process? != null) $'the spawn must be authorized: ($response)'
}

spawn $socket $headers { node: $proven, options: { tokens: { local: { authorization: [$token] } } } }
spawn $socket $headers { node: $granted, options: {} }

# Stopping the server drains the asynchronous grant writes.
server stop $server
let searches = open --raw $server.log
	| lines
	| skip $offset
	| where ($it | str starts-with '{')
	| each { from json }
	| where $it.fields.message? == 'authorize batch'
	| get fields.resource

assert (($searches | where $it == $granted | length) > 0) 'the log filter must observe a search that is genuinely required'
assert equal ($searches | where $it == $proven | length) 0 'the grant writer searched for a root it was already handed subtree permission for'
