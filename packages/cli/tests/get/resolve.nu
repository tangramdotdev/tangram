use ../../test.nu *

let server = spawn

let path = artifact {
	file: "hello"
}
let id = tg checkin $path
tg tag put foo $id

let tag = tg get foo | from json
assert equal $tag.specifier foo
assert equal $tag.item.id $id

let resolved = tg get -R foo | str trim
assert equal $resolved 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let resolved = tg resolve foo | str trim
assert equal $resolved 'tg.directory({"file":fil_01zxnj3x8es5hd13s3z91f9jy8e9ytqrgqvyt1h78v5fp8sc93ks60})'

let file = tg get -R --pretty "foo?get=file"
assert equal ($file | lines) [
	"tg.file({"
	'  "contents": blb_01t10ptmtyxpb108ztd4np15vt0jm9qnfkfny07vr8yp7tebj04dgg,'
	"})"
]
