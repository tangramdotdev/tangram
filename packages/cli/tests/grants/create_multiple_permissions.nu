use ../../test.nu *

# Granting several permissions in a single command confers all of them at once.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

# A one-shot read,write grant lets bob both read the group and perform the write operation of creating a subgroup.
tg --token $alice.token grant $bob.user.id read,write team
tg --token $bob.token group get team
tg --token $bob.token group create team/bob-sub
