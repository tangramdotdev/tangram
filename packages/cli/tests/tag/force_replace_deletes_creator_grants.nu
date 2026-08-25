use ../../test.nu *

# Force replacing a user deletes grants created by that user.

let root_token = random chars
let server = server spawn --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id read team
tg --token $bob.token group get team

let target = tg --token $root_token put 'tg.file("replacement")' | str trim
tg --token $root_token tag put --force alice $target

failure (tg --token $bob.token group get team | complete) "the grant created by the replaced user should be deleted"
let grants = tg --token $root_token grants list --resource team | from json
assert not (
	$grants | any {|grant|
		$grant.creator == $alice.user.id and $grant.subject == $bob.user.id
	}
) "the grant created by the replaced user should not be listed"
