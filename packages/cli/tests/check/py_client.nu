use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': '
        import asyncio
        import tangram
        from tangram import File
        from typing import assert_type
        async def default() -> tg.File:
            builder = tg.file().contents(asyncio.sleep(0, result="hello")).executable(True)
            file = await builder
            assert_type(file, File)
            assert_type(file, tangram.File)
            assert_type(await file.text(), str)
            directory = await tg.directory({"hello": tg.file("hello")})
            assert_type(directory, tangram.Directory)
            return file
    '
}
let file = $path | path join main.tg.py
success (tg check $file | complete)
let checked = tg checkin $file
rm --recursive $path
success (tg check $checked | complete)
success (tg run $checked | complete)

for source in [
    'tg.file().executable("wrong")'
    'value: tg.Directory = tg.File.with_id("fil_0000000000000000000000000000000000000000000000000000")'
    'import tangram; tangram.file().executable("wrong")'
    'tg.nonexistent()'
] {
    let path = artifact {'main.tg.py': $source}
    let output = tg check ($path | path join main.tg.py) | complete
    failure $output
    assert ($output.stderr | str contains 'main.tg.py:1:')
}
