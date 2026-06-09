use ../../test.nu *

# Putting a remote with an existing name overwrites its url.

let server = spawn

tg remote put upstream "http://localhost:9999"
tg remote put upstream "http://localhost:8888"

let got = tg remote get upstream | from json
assert equal $got.url "http://localhost:8888" "the second put should overwrite the url"

let list = tg remote list | from json
assert equal ($list | length) 1 "the overwrite should not create a second remote"
