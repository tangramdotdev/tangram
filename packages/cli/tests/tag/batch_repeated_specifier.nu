use ../../test.nu *

# A tag batch can repeat a specifier without treating its own earlier write as a snapshot mismatch.

let server = server spawn
let target = tg put 'tg.file("target")' | str trim
let body = {
	force: false,
	parents: false,
	tags: [
		{
			specifier: repeated,
			target: {
				id: $target,
				kind: object,
			},
		},
		{
			specifier: repeated,
			target: {
				id: $target,
				kind: object,
			},
		},
	],
} | to json --raw
let socket = $server.url | str replace 'http+unix://' '' | url decode
let status = (
	$body
	| into binary
	| http post
		--allow-errors
		--headers { 'Content-Type': 'application/json' }
		--unix-socket $socket
		'http://localhost/tags/batch'
	| metadata
	| get http_response.status
)

assert equal $status 200 "the repeated tag batch should succeed"
let tag = tg tag get repeated | from json
assert equal $tag.target.id $target

let replacement = tg put 'tg.file("replacement")' | str trim
let body = {
	force: true,
	parents: false,
	tags: [
		{
			specifier: forced,
			target: {
				id: $target,
				kind: object,
			},
		},
		{
			specifier: forced,
			target: {
				id: $replacement,
				kind: object,
			},
		},
	],
} | to json --raw
let status = (
	$body
	| into binary
	| http post
		--allow-errors
		--headers { 'Content-Type': 'application/json' }
		--unix-socket $socket
		'http://localhost/tags/batch'
	| metadata
	| get http_response.status
)

assert equal $status 200 "the forced repeated tag batch should succeed"
let tag = tg tag get forced | from json
assert equal $tag.target.id $replacement
