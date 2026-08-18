use ../../test.nu *

# Get preserves named nodes by default and follows them only when requested.

let server = spawn

let path = artifact {
	file: "hello"
}
let id = tg checkin $path
tg tag foo $id

let tag = tg get foo | from json
assert equal $tag.specifier foo
assert equal $tag.target.id $id

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
let group = tg get package | from json
assert equal $group.specifier package

let version = tg get "package/^1" | from json
assert equal $version.specifier package/1.0.0
assert equal $version.id (tg get package/1.0.0 | from json | get id)

let output = tg get "package?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let output = tg get $"($group.id)?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let output = tg get "package/^1?follow=true" | str trim
assert equal $output 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'
