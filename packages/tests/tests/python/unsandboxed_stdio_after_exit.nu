use ../../util/test.nu *

# Piped output remains readable after an unsandboxed Python client's child exits.

let local = server spawn
let path = artifact {
    'tangram.py': '
        async def default():
            process = await tg.spawn({"executable": "sh", "args": ["-c", "printf stdout; printf stderr >&2"]}).stdout("pipe").stderr("pipe")
            await process.wait()
            return {
                "stdout": await process.stdout.read_all_to_string(),
                "stderr": await process.stderr.read_all_to_string(),
            }
        async def dispose():
            process = await tg.spawn({"executable": "sh", "args": ["-c", "printf unread"]}).stdout("pipe")
            await process.wait()
            await process.close()
            assert await process.stdout.read() is None
            return "closed"
    '
}
let output = tg run $path | from json
assert equal $output { stdout: stdout, stderr: stderr } ($output | to json)

# Explicit disposal closes output that the caller has not consumed.
let output = tg run $'($path)#dispose' | from json
assert equal $output closed
