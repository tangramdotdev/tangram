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
import time
import uuid


# Adopt and reap the orphaned launcher processes.
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0
maps = json.loads(sys.argv[2])
name = 'tangram-test-' + uuid.uuid4().hex
cgroup = pathlib.Path(sys.argv[3]) / name
host, guest = socket.socketpair()
host.settimeout(5)
directory = tempfile.TemporaryDirectory()
config = pathlib.Path(directory.name) / 'setup.json'
config.write_text(json.dumps({
    'backend': {'kind': 'builtin', 'gid': os.getgid(), 'uid': os.getuid()},
    'command': {'args': ['/bin/sleep', '30']},
    'options': {'cgroup': {'name': name}, 'user_namespace_fd': guest.fileno()},
}))
process = subprocess.Popen([
    sys.argv[1], 'sandbox', 'run', '--config', str(config),
], env={**os.environ, 'TMPDIR': directory.name}, pass_fds=(guest.fileno(),))
guest.close()
child = None
workload_fd = None
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
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert process.poll() is None, 'the launcher exited before the workload'
        pending = [pid]
        while pending and child is None:
            parent = pending.pop()
            try:
                children = pathlib.Path(f'/proc/{parent}/task/{parent}/children').read_text().split()
            except FileNotFoundError:
                continue
            for candidate in map(int, children):
                try:
                    command = pathlib.Path(f'/proc/{candidate}/comm').read_text().strip()
                except FileNotFoundError:
                    continue
                if command == 'sleep':
                    child = candidate
                    break
                pending.append(candidate)
        if child is not None:
            break
        time.sleep(.01)
    assert child is not None, 'the workload did not start'
    workload_fd = os.pidfd_open(child)
    poll = select.poll()
    poll.register(workload_fd, select.POLLIN)
    assert not poll.poll(0), 'the workload exited before its launcher'
    process.kill()
    process.wait(timeout=5)
    assert poll.poll(5000), 'the mapped workload survived its launcher'

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
    if workload_fd is not None:
        os.close(workload_fd)
    host.close()
    directory.cleanup()
