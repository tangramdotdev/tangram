use ../../test.nu *

# Authorization diagnostics retain caller context across the index queue and distinguish tokens from graph lookups.

def get-object [socket: string, bearer: string, id: string, --token: string] {
	let query = if $token == null { '' } else { $'?tokens[local][authorization][0]=($token | url encode --all)' }
	http get --max-time 10sec --headers { Accept: 'application/json', Authorization: $'Bearer ($bearer)' } --unix-socket $socket $'http://localhost/objects/($id)($query)'
}

def call [event: record] {
	$event.spans | where name == authz_call | last
}

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	tracing: {
		filter: 'tangram=info,tangram_authz=trace'
		stderr_format: json
		stderr_span_events: false
	}
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let socket = $server.url | str replace 'http+unix://' '' | url decode
let directory = tg --token $alice.token put 'tg.directory({ "child": tg.file("authz tracing") })' | str trim
tg --token $alice.token index

let first = get-object $socket $alice.token $directory
let token = $first.tokens.local.authorization.0
get-object $socket $bob.token $directory --token $token | ignore
failure (tg --token $bob.token object get $directory | complete) 'Bob should still need a token.'
let path = artifact { tangram.ts: 'export default () => tg.file("traced build");' }
tg --token $alice.token build $path | ignore
server stop $server

let log = open --raw $server.log
let events = $log | lines | where ($it | str starts-with '{') | each { from json }
let events = $events | where ($it.target | str starts-with tangram_authz)
assert not ($log | str contains $token) 'Diagnostics must not contain bearer authorization tokens.'
assert not ($log | str contains $alice.token) 'Diagnostics must not contain authentication tokens.'
assert not ($events | any { $in.fields.message? in [new enter exit close] }) 'Span lifecycle events should be disabled.'

let missing = $events | where $it.fields.message? == 'authz.index_required' | where $it.fields.resource? == $directory
assert ($missing | any { $in.fields.reason == missing_tokens }) 'The missing token must be reported.'
let lookup = $missing | first
let lookup_call = call $lookup
assert ($lookup_call.caller | str contains 'packages/server/src/object/get.rs:') 'The caller should be the object operation, not the authorization wrapper.'
let correlated = $events | where { |event| ($event.spans? | default [] | any { $in.authz_id? == $lookup_call.authz_id }) }
assert ($correlated | any { $in.fields.message? == 'authz.index_finish' and $in.fields.reads? > 0 }) 'Index costs must retain the caller context.'
assert ($correlated | any { $in.fields.message? == 'authz.fact' }) 'Detailed facts must retain the caller context.'
assert ($correlated | any { $in.fields.message? == 'authz.finish' and $in.span.name? == authz_call and $in.fields.status? == ok }) 'The call must have a timed completion.'

let proof = $events | where $it.fields.message? == 'authz.resource_result' | where $it.fields.resource? == $directory | where $it.fields.path? == proof | last
let proof_call = call $proof
let proof_events = $events | where { |event| ($event.spans? | default [] | any { $in.authz_id? == $proof_call.authz_id }) }
assert ($proof_events | any { $in.fields.message? == 'authz.resource' and $in.fields.tokens? == 1 }) 'The incoming token count must be recorded.'
assert ($proof_events | any { $in.fields.message? == 'authz.token_verify' and $in.fields.reason? == valid }) 'The token verification result must be recorded.'
assert not ($proof_events | any { $in.fields.message? == 'authz.index_start' }) 'The exact token should avoid the index.'

let grants = $events | where $it.fields.message? == 'authz.grant_root'
assert not ($grants | is-empty) 'Build authorization in grant write transactions must be recorded.'
assert ($grants | all { $in.spans | any { $in.name == authz_process_grants } }) 'Grant authorization must identify the process and backend.'
