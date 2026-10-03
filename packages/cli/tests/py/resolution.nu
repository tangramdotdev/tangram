use ../lib/test.nu *

let local = server spawn
let path = artifact {
    'main.tg.py': '
        import importlib
        from .left import entry as left
        from .right import entry as right
        assert left.value == 1
        assert right.value == 2
        assert left.module is not right.module
        for _ in range(2):
            try:
                importlib.import_module(".broken", __package__)
            except RuntimeError as error:
                assert str(error) == "failed initialization"
            else:
                assert False
        print("resolution completed")
    '
    left: {
        'entry.tg.py': 'from . import helper as module; value = module.value'
        'helper.tg.py': 'value = 1'
    }
    right: {
        'entry.tg.py': 'from . import helper as module; value = module.value'
        'helper.tg.py': 'value = 2'
    }
    'broken.tg.py': 'raise RuntimeError("failed initialization")'
}
let output = tg py ($path | path join main.tg.py) | complete
success $output
assert equal ($output.stdout | str trim) 'resolution completed'

let missing = artifact {'main.tg.py': 'from . import missing'}
let output = tg py ($missing | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'missing')

let ambiguous = artifact {
    'main.tg.py': 'from . import helper'
    'helper.tg.py': 'value = 1'
    helper: {'tangram.py': 'value = 2'}
}
let output = tg py ($ambiguous | path join main.tg.py) | complete
failure $output
assert ($output.stderr | str contains 'ambiguous Tangram module')
