use ../lib/test.nu *

let local = server spawn

# Moving resolution to Rust must preserve lazy loading, canonical identity, and retries.
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.child]
        # specifier = "./child.tg.py"
        # [tool.tangram.imports.same]
        # specifier = "./child.tg.py"
        # [tool.tangram.imports.broken]
        # specifier = "./broken.tg.py"
        # [tool.tangram.imports.pkg]
        # specifier = "./pkg"
        # ///
        import importlib.util
        import sys
        import builtins
        builtins.calls = 0
        builtins.attempts = 0
        builtins.packages = 0
        spec = importlib.util.find_spec("child")
        module = importlib.util.module_from_spec(spec)
        assert importlib.util.module_from_spec(spec) is module
        assert builtins.calls == 0
        assert "value = 42" in spec.loader.get_source(spec.name)
        assert builtins.calls == 0
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        spec.loader.exec_module(module)
        import child, same
        assert child is same is module and child.value == 42
        assert builtins.calls == 1
        for attempt in range(2):
            try:
                import broken
            except RuntimeError:
                assert attempt == 0
            else:
                assert broken.value == 8
        try:
            import pkg
        except RuntimeError:
            pass
        import pkg
        assert pkg.sub.read() == 2
        assert builtins.packages == 2
        def default():
            return child.value
    '
    'child.tg.py': 'import builtins; builtins.calls += 1; value = 42'
    'broken.tg.py': '
        import builtins
        builtins.attempts += 1
        if builtins.attempts == 1:
            raise RuntimeError("retry")
        value = 8
    '
    pkg: {
        'tangram.py': '
            import builtins
            builtins.packages += 1
            value = builtins.packages
            from . import sub
            if value == 1:
                raise RuntimeError("retry")
        '
        sub: {'tangram.py': 'def read():
    from .. import value
    return value'}
    }
}
success (tg py --export default ($path | path join main.tg.py) | complete)
let module = tg checkin ($path | path join main.tg.py)
rm --recursive $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '42'

# A child entry must initialize its package before executing, even through a cycle.
let path = artifact {
    'tangram.py': 'value = 42
from . import child'
    'child.tg.py': '
        from . import value
        calls = globals().get("calls", 0) + 1
        assert calls == 1
        def default():
            return value
    '
}
success (tg py --export default ($path | path join child.tg.py) | complete)
let module = tg checkin ($path | path join child.tg.py)
rm --recursive $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '42'

# A failed containing package must be retried before a declared child executes.
let path = artifact {
    'main.tg.py': '
        # /// script
        # [tool.tangram.imports.child]
        # specifier = "./pkg/child.tg.py"
        # ///
        import builtins
        builtins.attempts = 0
        try:
            import child
        except RuntimeError:
            pass
        import child
        assert child.value == 42 and builtins.attempts == 2
        def default():
            return child.value
    '
    pkg: {
        'tangram.py': '
            import builtins
            builtins.attempts += 1
            if builtins.attempts == 1:
                raise RuntimeError("retry")
            value = 42
        '
        'child.tg.py': 'from . import value'
    }
}
success (tg py --export default ($path | path join main.tg.py) | complete)
let module = tg checkin ($path | path join main.tg.py)
rm --recursive $path
let output = tg run $module | complete
success $output
assert equal ($output.stdout | str trim) '42'
