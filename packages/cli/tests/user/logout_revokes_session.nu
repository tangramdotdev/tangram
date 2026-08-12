use ../../test.nu *

# Logging out removes the local token and revokes the server-side session.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
tg logout

let local = tg user whoami | complete
failure $local "logout should remove the local session"

let revoked = tg --token $alice.token user whoami | complete
failure $revoked "logout should revoke the session on the server"
