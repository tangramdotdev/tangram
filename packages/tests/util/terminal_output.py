"""Capture a command's output through a foreground controlling terminal."""

import errno
import os
import pty
import select
import signal
import sys
import termios


def main():
    pid, terminal = pty.fork()
    if pid == 0:
        termios.tcsetwinsize(0, (24, 80))
        os.execvp(sys.argv[1], sys.argv[1:])
    output = bytearray()
    finished = False
    try:
        while True:
            readable, _, _ = select.select([terminal], [], [], 20)
            if not readable:
                raise AssertionError(f"the command stopped producing output: {output!r}")
            try:
                chunk = os.read(terminal, 65536)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
            if not chunk:
                break
            output.extend(chunk)
        _, status = os.waitpid(pid, 0)
        finished = True
        sys.stdout.buffer.write(output)
        return os.waitstatus_to_exitcode(status)
    finally:
        if not finished:
            os.killpg(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        os.close(terminal)


if __name__ == "__main__":
    sys.exit(main())
