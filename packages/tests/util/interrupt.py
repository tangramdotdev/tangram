"""Check Ctrl-C cleanup and signal restoration through a controlling terminal."""

import errno
import os
import pty
import select
import signal
import sys
import time


def main():
    pid, terminal = pty.fork()
    if pid == 0:
        os.execvp(sys.argv[1], sys.argv[1:])
    output = b""
    interrupted = False
    restored = False
    status = None
    deadline = time.monotonic() + 20
    try:
        while time.monotonic() < deadline:
            readable, _, _ = select.select([terminal], [], [], 0.05)
            if readable:
                try:
                    chunk = os.read(terminal, 65536)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    chunk = b""
                output += chunk
                if b"child ready\r\n" in output and not interrupted:
                    os.write(terminal, b"\x03")
                    interrupted = True
                if b"parent ready\r\n" in output and not restored:
                    assert b"stopping one\r\nstopping two\r\n" in output, output
                    os.write(terminal, b"\x03")
                    restored = True
            finished, status = os.waitpid(pid, os.WNOHANG)
            if finished:
                assert interrupted and restored, output
                assert os.waitstatus_to_exitcode(status) != 0, output
                return
        raise AssertionError(f"Ctrl-C did not finish the process: {output!r}")
    finally:
        if status is None or not finished:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            os.waitpid(pid, 0)
        os.close(terminal)


if __name__ == "__main__":
    main()
