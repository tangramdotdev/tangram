use ../lib/test.nu *

# Python escapes keywords without displacing existing exports or changing command targets.

let local = server spawn
let path = artifact {
    'tools.tg.ts': '
        export const assert = (value: unknown) => `assert:${value}`;
        export const globals = () => "globals";
        export const lambda = () => "lambda";
        export const lambda_ = () => "lambda_";
        export const lambda__ = () => "lambda__";
        export const match = () => "match";
        export const other = () => "other";
    '
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.tools]
        # specifier = "./tools.tg.ts"
        # ///

        import functools
        import tools
        from tools import assert_ as check

        @functools.wraps(tools.assert_)
        async def wrapped(value):
            return f"wrapped:{await tools.assert_(value)}"

        async def default():
            assert tools.__all__ == ["assert_", "globals", "lambda_", "lambda__", "lambda___", "match", "other"]
            assert tg.host.magic(tools.assert_)["export"] == "assert"
            assert await tg.build(check, "builder") == "assert:builder"
            for binding, export in [("globals", "globals"), ("lambda_", "lambda_"), ("lambda__", "lambda__"), ("lambda___", "lambda"), ("match", "match")]:
                function = getattr(tools, binding)
                assert tg.host.magic(function)["export"] == export
                assert await function() == export
                assert await tg.build(function) == export
            assert await tools.other() == "other"
            target = tg.host.magic(wrapped)
            assert target["module"]["kind"] == "python"
            assert target["export"] == "wrapped"
            assert await wrapped("direct") == "wrapped:assert:direct"
            assert await tg.build(wrapped, "builder") == "wrapped:assert:builder"
            return await tools.assert_("value")
    '
}
let file = $path | path join main.tg.py
success (tg check $file | complete)
let output = tg build $file | complete
success $output
assert equal ($output.stdout | str trim) '"assert:value"'
