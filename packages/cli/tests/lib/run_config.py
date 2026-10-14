import os
import pathlib
import subprocess
import sys
import threading


contents = pathlib.Path(sys.argv[2]).read_bytes()
reader, writer = os.pipe()
errors = []


def write_config():
    try:
        with os.fdopen(writer, 'wb') as pipe:
            pipe.write(contents)
    except OSError as error:
        errors.append(error)


thread = threading.Thread(target=write_config)
thread.start()
try:
    process = subprocess.Popen(
        [sys.argv[1], 'sandbox', 'run', '--config-fd', str(reader)],
        pass_fds=(reader,),
    )
finally:
    os.close(reader)
status = process.wait(timeout=30)
thread.join(timeout=5)
assert not thread.is_alive(), 'the configuration writer did not complete'
assert not errors, errors
sys.exit(status)
