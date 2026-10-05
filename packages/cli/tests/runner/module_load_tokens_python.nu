use ../lib/test.nu *
use ../lib/vfs.nu

vfs skip_unless_supported

# A loaded module must retain the tokens needed to push a child command.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: false, final: false } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
	verification: { permissions: { initial: false, final: false } },
	vfs: true,
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# A Python child command must keep the loaded module's authorization tokens.
let path = artifact {
    'tangram.py': '
        def child():
            return 1
        async def default():
            return await tg.build(child)
    '
}
let output = timeout 20s tg --url $local.url build --remote $path | complete
success $output 'the python parent should be able to wait for its child'
assert equal ($output.stdout | str trim) '1'

# Reimporting a graph module must retain the tokens returned by its first load.
let path = artifact {
    'tangram.py': '
        value = 1
        from .child import child
        async def default():
            from .child import child as again
            assert again is child
            return await tg.build(child)
    '
    'child.tg.py': '
        from . import value
        def child():
            return value
    '
}
let output = timeout 20s tg --url $local.url build --remote $path | complete
success $output 'the python graph module should retain its tokens after another import'
assert equal ($output.stdout | str trim) '1'
