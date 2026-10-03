use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': '
        import asyncio
        import os
        import tangram as tg

        async def nested(value):
            await asyncio.sleep(0)
            return value

        async def run(*args):
            assert args == ("one", True, "two", 3, "trailing")
            assert tg.process.args == list(args)
            assert tg.process.cwd == os.getcwd()
            assert tg.process.export == "run"
            assert tg.process.module.kind == "py"
            assert tg.process.module.referent.node == __file__
            assert tg.process.env["TANGRAM_URL"]
            file = await tg.file(nested("hello")).contents(nested("world"))
            print(await file.text())
            return {"answer": nested(42), "file": file}
    '
}
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} {
    tg py --export run -a one -A true -a two -A 3 ($path | path join main.tg.py) trailing | complete
}
success $output
assert equal ($output.stdout | str trim) 'helloworld'
let outcome = open --raw $output_path | from json
assert equal $outcome.exit 0
assert equal $outcome.output.kind 'map'
assert equal $outcome.output.value.answer 42
assert equal $outcome.output.value.file.kind 'object'
assert equal (xattr_read 'user.tangram.outcome' $output_path | str length) 0

# Value exports, null outputs, and task cancellation use the same outcome contract.
let values = artifact {
    'main.tg.py': '
        import asyncio
        import tangram as tg
        answer = asyncio.get_running_loop().create_future()
        answer.set_result({"ready": True})

        async def background():
            try:
                await asyncio.Event().wait()
            finally:
                print("background canceled")

        async def nothing():
            asyncio.create_task(background())
            await asyncio.sleep(0)
    '
}
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} {
    tg py --export answer ($values | path join main.tg.py) | complete
}
success $output
assert equal ((open --raw $output_path | from json).output.value.ready) true
let output_path = mktemp
let output = with-env {TANGRAM_OUTPUT: $output_path} {
    tg py --export nothing ($values | path join main.tg.py) | complete
}
success $output
assert equal ((open --raw $output_path | from json).output) null
assert equal ($output.stdout | str trim) 'background canceled'
