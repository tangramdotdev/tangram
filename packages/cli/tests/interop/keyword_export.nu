use ../lib/test.nu *

# Python escapes keywords without displacing existing exports or changing command targets, and omits exports that it cannot name.

let local = server spawn
let path = artifact {
    'tools.tg.ts': '
        export const $ = () => "$";
        export const assert = (value: unknown) => `assert:${value}`;
        export const lambda = () => "lambda";
        export const lambda_ = () => "lambda_";
        export const lambda__ = () => "lambda__";
        export const match = () => "match";
    '
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.tools]
        # specifier = "./tools.tg.ts"
        # ///

        import functools
        import tools

        @functools.wraps(tools.assert_)
        async def wrapped(value):
            return f"wrapped:{await tools.assert_(value)}"

        async def default():
            assert tools.__all__ == ["assert_", "lambda_", "lambda__", "lambda___", "match"]
            assert await tools.lambda_() == "lambda_"
            assert await tools.lambda__() == "lambda__"
            assert await tools.lambda___() == "lambda"
            assert await tools.match() == "match"
            assert await tg.build(wrapped, "builder") == "wrapped:assert:builder"
            return await tools.assert_("value")
    '
}
let file = $path | path join main.tg.py
success (tg check $file | complete)
let output = tg build $file | complete
success $output
assert equal ($output.stdout | str trim) '"assert:value"'
