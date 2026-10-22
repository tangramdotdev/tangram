import ctypes
import json
import os
import pathlib
import select
import socket
import struct
import subprocess
import sys
import tempfile
import uuid


# Adopt and reap the orphaned launcher processes.
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0
maps = json.loads(sys.argv[2])
name = 'tangram-test-' + uuid.uuid4().hex
cgroup = pathlib.Path(sys.argv[3]) / name
host, guest = socket.socketpair()
output_reader, output_writer = os.pipe()
host.settimeout(5)
directory = tempfile.TemporaryDirectory()
config = pathlib.Path(directory.name) / 'setup.json'
config.write_text(json.dumps({
    'backend': {'kind': 'builtin', 'gid': os.getgid(), 'uid': os.getuid()},
    'command': {'args': ['/bin/sh', '-c', 'echo ready; exec /bin/sleep 30']},
    'options': {'cgroup': {'name': name}, 'user_namespace_fd': guest.fileno()},
}))
process = subprocess.Popen([
    sys.argv[1], 'sandbox', 'run', '--config', str(config),
], env={**os.environ, 'TMPDIR': directory.name}, pass_fds=(guest.fileno(),),
    stdout=output_writer)
guest.close()
os.close(output_writer)
try:
    pid = struct.unpack('i', host.recv(4))[0]
    for kind, identity in [('uid', os.getuid()), ('gid', os.getgid())]:
        mapping = maps[kind + '_map']
        count = mapping['count']
        if os.geteuid() == 0:
            pathlib.Path(f'/proc/{pid}/{kind}_map').write_text(
                f"0 {mapping['host']} {count}\n{count} {identity} 1\n"
            )
        else:
            subprocess.run([
                mapping['helper'], str(pid), '0', str(mapping['host']),
                str(count), str(count), str(identity), '1',
            ], check=True)
    host.sendall(b'0')

    # Wait for the workload to start, then kill its launcher and wait for every holder of the workload's standard output to exit.
    poll = select.poll()
    poll.register(output_reader, select.POLLIN)
    assert poll.poll(5000), 'the workload did not start'
    assert os.read(output_reader, 6) == b'ready\n', 'the workload did not start'
    assert process.poll() is None, 'the launcher exited before the workload'
    process.kill()
    process.wait(timeout=5)
    assert poll.poll(5000) and os.read(output_reader, 1) == b'', 'the mapped workload survived its launcher'
finally:
    if cgroup.exists():
        (cgroup / 'cgroup.kill').write_text('1\n')
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
    while True:
        try:
            os.waitpid(-1, 0)
        except ChildProcessError:
            break
    if cgroup.exists():
        cgroup.rmdir()
    os.close(output_reader)
    host.close()
    directory.cleanup()
