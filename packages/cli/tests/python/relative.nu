use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'tangram.py': '
        import importlib
        from . import helper, cycle_a, sub, namespace
        from .helper import value
        from .namespace import leaf

        def default():
            assert value == 42
            assert cycle_a.result == "a:b"
            assert sub.value == 43
            assert leaf.value == 44
            assert helper is importlib.import_module(".helper", __package__)
            assert helper.calls == 1
            assert helper.__tangram_module__.kind == "python"
            assert __spec__.origin == __file__
            print("relative modules completed")
    '
    'helper.tg.py': '
        calls = globals().get("calls", 0) + 1
        value = 42
    '
    'cycle_a.tg.py': '
        name = "a"
        from . import cycle_b
        result = name + ":" + cycle_b.name
    '
    'cycle_b.tg.py': '
        from . import cycle_a
        assert cycle_a.name == "a"
        name = "b"
    '
    sub: {
        'tangram.py': '
            from ..helper import value as parent
            from . import child
            value = parent + child.value
        '
        'child.tg.py': 'value = 1'
    }
    namespace: {
        'leaf.tg.py': '
            from ..helper import value as parent
            value = parent + 2
        '
    }
}
let output = tg python --export default ($path | path join tangram.py) | complete
success $output
assert equal ($output.stdout | str trim) 'relative modules completed'

# A non-root entry in an explicit package gets the same package context.
let entry = artifact {
    'tangram.py': 'pass'
    'my-script.v1.tg.py': 'from .helper import value; from .__entry__ import sentinel; assert sentinel == 7; print(value)'
    '__entry__.tg.py': 'sentinel = 7'
    'helper.tg.py': 'value = 99'
}
let output = tg python ($entry | path join my-script.v1.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) '99'

# A child entry initializes its package and shares the initializer's child import.
let entry = artifact {
    'tangram.py': 'value = 42; from . import child'
    'child.tg.py': '
        from . import value
        calls = globals().get("calls", 0) + 1
        def default():
            assert calls == 1
            print(value)
    '
}
let output = tg python --export default ($entry | path join child.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) '42'

# Explicit namespace directory imports retain directory traversal after checkin.
let entry = artifact {
    'tangram.py': '
        import importlib
        namespace = importlib.import_module(".namespace", __package__)
        leaf = importlib.import_module(".leaf", namespace.__name__)
        def default():
            return leaf.value
    '
    namespace: {'leaf.tg.py': 'value = 42'}
}
success (tg python --export default ($entry | path join tangram.py) | complete)
let checked = tg checkin ($entry | path join tangram.py)
rm --recursive $entry
assert equal (tg run $checked | str trim) '42'
