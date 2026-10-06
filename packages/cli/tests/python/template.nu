use ../lib/test.nu *

const python_path = path self '../../../../.venv/bin/python'

let local = server spawn
let path = artifact {
    'main.tg.py': '
        import asyncio
        import tangram as tg

        async def default() -> tg.File:
            input_file = tg.file("input")
            template = await tg.template(t"cat {input_file} > {tg.output}")
            assert template.components[0] == "cat "
            artifact = template.components[1]
            assert isinstance(artifact, tg.File)
            assert template.components[2] == " > "
            assert template.components[3] is tg.output
            assert await artifact.text() == "input"
            name = asyncio.sleep(0, result="world")
            builder = tg.file(t"""
                Hello, {name}!
            """)
            file = await builder
            assert (await builder).id == file.id
            assert await file.text() == "Hello, world!\n"
            return file
    '
}
let module = $path | path join main.tg.py
success (tg check $module | complete)
let standalone = ^$python_path -c 'import asyncio, runpy, sys; asyncio.run(runpy.run_path(sys.argv[1])["default"]())' $module | complete
success $standalone
let output = tg run $module | complete
success $output
let file = tg checkout ($output.stdout | str trim)
assert equal (open --raw $file) "Hello, world!\n"
