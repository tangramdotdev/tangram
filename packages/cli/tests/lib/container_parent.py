import ctypes
import json
import os
import pathlib
import socket
import struct
import subprocess
import sys
import time
import uuid


# Adopt and reap the workload even when its launcher is killed.
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0
maps = json.loads(sys.argv[2])
name = 'tangram-test-' + uuid.uuid4().hex
cgroup = pathlib.Path(sys.argv[3]) / name
host, guest = socket.socketpair()
host.settimeout(5)
process = subprocess.Popen([
    sys.argv[1], 'sandbox', 'container', 'run', '--index', '0',
    '--unshare-all', '--as-pid-1', '--die-with-parent',
    '--uid', str(os.getuid()), '--gid', str(os.getgid()), '--chdir', '/',
    '--cgroup', name, '--user-namespace-fd', str(guest.fileno()),
    '--', '/bin/sleep', '30',
], pass_fds=(guest.fileno(),))
guest.close()
child = None
reaped = False
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
        children = pathlib.Path(f'/proc/{pid}/task/{pid}/children').read_text().split()
        if children:
            candidate = int(children[0])
            if pathlib.Path(f'/proc/{candidate}/comm').read_text().strip() == 'sleep':
                child = candidate
                break
        time.sleep(.01)
    assert child is not None, 'the workload did not start'
    process.kill()
    process.wait(timeout=5)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        pid, status = os.waitpid(child, os.WNOHANG)
        if pid == child:
            reaped = True
            assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == 9
            break
        time.sleep(.01)
    assert reaped, 'the mapped workload survived its launcher'
finally:
    if cgroup.exists():
        (cgroup / 'cgroup.kill').write_text('1\n')
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
    if child is not None and not reaped:
        os.waitpid(child, 0)
    if cgroup.exists():
        cgroup.rmdir()
    host.close()
