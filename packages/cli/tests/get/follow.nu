use ../../test.nu *

# Get preserves named nodes by default and follows them only when requested.

let server = server spawn

let path = artifact {
	file: "hello"
}
let id = tg checkin $path
tg tag foo $id

let output = with-env { TANGRAM_QUIET: "false" } { tg get foo | complete }
success $output
let tag = $output.stdout | from json
assert equal $tag.specifier foo
assert equal $tag.target.id $id
assert (($tag | get --optional location) == null) "the location should not be printed to stdout"
assert (($tag | get --optional tokens) == null) "the tokens should not be printed to stdout"
assert ($output.stderr | str contains $tag.id) "the referent should be printed as an info message"

let output = tg get "foo?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let output = tg get $"($tag.id)?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let file = tg get --pretty "foo?follow=true&get=file"
assert equal ($file | lines) [
	"tg.file({"
	'  "contents": blb_01t10ptmtyxpb108ztd4np15vt0jm9qnfkfny07vr8yp7tebj04dgg,'
	"})"
]

tg tag -p package/1.0.0 $id
let output = with-env { TANGRAM_QUIET: "false" } { tg get package | complete }
success $output
let group = $output.stdout | from json
assert equal $group.specifier package
assert (($group | get --optional location) == null) "the location should not be printed to stdout"
assert (($group | get --optional tokens) == null) "the tokens should not be printed to stdout"
assert ($output.stderr | str contains $group.id) "the referent should be printed as an info message"

let version = tg get "package/^1" | from json
assert equal $version.specifier package/1.0.0
assert equal $version.id (tg get package/1.0.0 | from json | get id)

let output = tg get "package?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let output = tg get $"($group.id)?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let output = tg get "package/^1?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'
