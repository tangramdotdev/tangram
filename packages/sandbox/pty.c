#define _XOPEN_SOURCE 600
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <termios.h>
#include <sys/select.h>
#include <sys/ioctl.h>
#include <string.h>
#include <signal.h>

struct termios orig_termios; // Save original terminal settings
int master_fd;              // Master side of PTY

// Reset terminal to original state
void reset_terminal() {
    tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios);
}

// Set terminal to raw mode for proper PTY handling
void set_raw_mode() {
    struct termios raw;

    // Save original terminal settings
    if (tcgetattr(STDIN_FILENO, &orig_termios) == -1) {
        perror("tcgetattr");
        exit(1);
    }

    // Register reset function at exit
    atexit(reset_terminal);

    // Modify settings for raw mode
    raw = orig_termios;
    raw.c_iflag &= ~(ICRNL | IXON);
    raw.c_oflag &= ~(OPOST);
    raw.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);

    // Set new terminal settings
    if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) == -1) {
        perror("tcsetattr");
        exit(1);
    }
}

// Handle window size changes
void set_winsize(int fd, int rows, int cols) {
    struct winsize ws;

    ws.ws_row = rows;
    ws.ws_col = cols;
    ws.ws_xpixel = 0;
    ws.ws_ypixel = 0;

    if (ioctl(fd, TIOCSWINSZ, &ws) < 0) {
        perror("ioctl TIOCSWINSZ");
    }
}

// Signal handler for window size changes
void sigwinch_handler(int sig) {
    struct winsize ws;

    // Get current window size
    if (ioctl(STDIN_FILENO, TIOCGWINSZ, &ws) < 0) {
        perror("ioctl TIOCGWINSZ");
        return;
    }

    // Set the same size for the PTY
    set_winsize(master_fd, ws.ws_row, ws.ws_col);
}

int main(int argc, char *argv[]) {
    pid_t pid;
    char *shell;
    int status;
    fd_set readfds;
    char buf[256];
    struct winsize ws;

    // Get current window size
    if (ioctl(STDIN_FILENO, TIOCGWINSZ, &ws) < 0) {
        perror("ioctl TIOCGWINSZ");
        exit(1);
    }

    // Set terminal to raw mode
    set_raw_mode();

    // Open pseudoterminal master
    master_fd = posix_openpt(O_RDWR);
    if (master_fd < 0) {
        perror("posix_openpt");
        exit(1);
    }

    // Grant access to slave PTY
    if (grantpt(master_fd) < 0) {
        perror("grantpt");
        exit(1);
    }

    // Unlock slave PTY
    if (unlockpt(master_fd) < 0) {
        perror("unlockpt");
        exit(1);
    }

    // Fork a child process
    pid = fork();

    if (pid < 0) {
        // Fork failed
        perror("fork");
        exit(1);
    } else if (pid == 0) {
        pid = fork();
        if (pid < 0) {
          perror("doublefork");
          exit(1);
        }
        if (pid == 0) {
          // Open the slave PTY
          // Need to get slave name before closing master_fd
          char *slave_name = ptsname(master_fd);
          if (slave_name == NULL) {
              perror("ptsname");
              exit(1);
          }

          int slave_fd = open(slave_name, O_RDWR);
          if (slave_fd < 0) {
              perror("open slave pty");
              exit(1);
          }

          // Create a new session and set the slave PTY as the controlling terminal
          if (setsid() < 0) {
              perror("setsid");
              exit(1);
          }

          if (ioctl(slave_fd, TIOCSCTTY, 0) < 0) {
              perror("ioctl TIOCSCTTY");
              exit(1);
          }

          // Close the master.
          close(master_fd);

          // Redirect stdin, stdout, stderr to the slave PTY
          dup2(slave_fd, STDIN_FILENO);
          dup2(slave_fd, STDOUT_FILENO);
          // dup2(slave_fd, STDERR_FILENO);

          if (slave_fd > STDERR_FILENO) {
              close(slave_fd);
          }

          // Set window size for the slave PTY
          set_winsize(STDIN_FILENO, ws.ws_row, ws.ws_col);

          // Execute the shell or specified command
          shell = getenv("SHELL");
          if (shell == NULL) {
              shell = "/bin/sh";
          }

          if (argc > 1) {
              // If command was specified, use it
              execvp(argv[1], &argv[1]);
          } else {
              // Otherwise use default shell
              execlp(shell, shell, NULL);
          }

          // If exec fails
          perror("exec");
          exit(1);
        }


    } else {
        // Parent process

        // Set up signal handler for window size changes
        signal(SIGWINCH, sigwinch_handler);

        // Main I/O loop
        while (1) {
            int ret;

            FD_ZERO(&readfds);
            FD_SET(STDIN_FILENO, &readfds);
            FD_SET(master_fd, &readfds);

            // Wait for data from stdin or from the PTY
            ret = select(master_fd + 1, &readfds, NULL, NULL, NULL);

            if (ret < 0 && errno != EINTR) {
                perror("select");
                exit(1);
            }

            if (ret > 0) {
                // If data on stdin, send it to the PTY
                if (FD_ISSET(STDIN_FILENO, &readfds)) {
                    int bytes = read(STDIN_FILENO, buf, sizeof(buf));
                    if (bytes <= 0) {
                        break;
                    }

                    // Write to master PTY - this sends data to child's stdin
                    if (write(master_fd, buf, bytes) != bytes) {
                        perror("write to master PTY");
                    }
                }

                // If data on PTY, send it to stdout
                if (FD_ISSET(master_fd, &readfds)) {
                    int bytes = read(master_fd, buf, sizeof(buf));
                    if (bytes <= 0) {
                        // Child has closed the PTY or an error occurred
                        break;
                    }

                    // Write to our stdout - this sends child's output to parent's stdout
                    if (write(STDOUT_FILENO, buf, bytes) != bytes) {
                        perror("write to stdout");
                    }
                }
            }
        }

      // Wait for child process to exit
      waitpid(pid, &status, 0);
    }

    return 0;
}