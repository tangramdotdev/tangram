import os
import pty
import sys
import threading


def drain(fd):
    while True:
        try:
            if not os.read(fd, 4096):
                return
        except OSError:
            return


def main():
    trigger_path = sys.argv[1]
    command = sys.argv[2:]
    os.mkfifo(trigger_path)
    pid, fd = pty.fork()
    if pid == 0:
        os.execvp(command[0], command)

    # Drain the terminal output in the background so the child never blocks.
    threading.Thread(target=drain, args=(fd,), daemon=True).start()

    # Wait for the trigger to be written to the FIFO, then type q.
    with open(trigger_path, "rb") as trigger:
        trigger.read(1)
    os.write(fd, b"q\n")

    # Wait for the child to exit.
    _, status = os.waitpid(pid, 0)
    exit_code = os.waitstatus_to_exitcode(status)

    return exit_code if exit_code >= 0 else 128 - exit_code


if __name__ == "__main__":
    sys.exit(main())
