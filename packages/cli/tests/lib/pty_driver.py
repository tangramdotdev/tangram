import os
import pty
import select
import sys
import time


def drain(fd):
    while select.select([fd], [], [], 0)[0]:
        try:
            if not os.read(fd, 4096):
                return
        except OSError:
            return


def main():
    pid_path = sys.argv[1]
    trigger_path = sys.argv[2]
    command = sys.argv[3:]
    pid, fd = pty.fork()
    if pid == 0:
        os.execvp(command[0], command)

    with open(pid_path, "w", encoding="utf-8") as file:
        file.write(str(pid))
        file.flush()

    while not os.path.exists(trigger_path):
        drain(fd)
        time.sleep(0.01)
    os.write(fd, b"q\n")

    while True:
        drain(fd)
        result, status = os.waitpid(pid, os.WNOHANG)
        if result == pid:
            break
        time.sleep(0.01)
    exit_code = os.waitstatus_to_exitcode(status)

    return exit_code if exit_code >= 0 else 128 - exit_code


if __name__ == "__main__":
    sys.exit(main())
