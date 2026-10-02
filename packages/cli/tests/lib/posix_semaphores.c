#include <errno.h>
#include <fcntl.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

static int unlink_semaphore(const char *name) {
	if (sem_unlink(name) != 0 && errno != ENOENT) {
		perror(name);
		return 1;
	}
	return 0;
}

static int lock_file_semaphores(const char *path, int names_only) {
	struct stat status;
	if (stat(path, &status) != 0) {
		perror(path);
		return 1;
	}
	/* Match LMDB's default POSIX name: FNV-1a of the padded device/inode pair. */
	struct { dev_t device; ino_t inode; } identity;
	memset(&identity, 0, sizeof(identity));
	identity.device = status.st_dev;
	identity.inode = status.st_ino;
	uint64_t hash = UINT64_C(0xcbf29ce484222325);
	const unsigned char *bytes = (const unsigned char *)&identity;
	for (size_t i = 0; i < sizeof(identity); i++) {
		hash = (hash ^ bytes[i]) * UINT64_C(0x100000001b3);
	}
	const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~";
	char name[16] = "/MDBr";
	size_t position = 5;
	while (hash != 0 && position < 15) {
		name[position++] = alphabet[hash % 85];
		hash /= 85;
	}
	name[position] = '\0';
	if (names_only) {
		puts(name);
		name[4] = 'w';
		puts(name);
		return 0;
	}
	if (unlink_semaphore(name) != 0) {
		return 1;
	}
	name[4] = 'w';
	return unlink_semaphore(name);
}

int main(int argc, char **argv) {
	int create = argc > 1 && strcmp(argv[1], "--create") == 0;
	int exists = argc > 1 && strcmp(argv[1], "--exists") == 0;
	int lock_file = argc > 1 && strcmp(argv[1], "--lock-file") == 0;
	int names_only = argc > 1 && strcmp(argv[1], "--lock-file-names") == 0;
	int start = create || exists || lock_file || names_only ? 2 : 1;
	for (int i = start; i < argc; i++) {
		if (lock_file || names_only) {
			if (lock_file_semaphores(argv[i], names_only) != 0) {
				return 1;
			}
		} else if (create || exists) {
			sem_t *semaphore = create
				? sem_open(argv[i], O_CREAT | O_EXCL, 0600, 1)
				: sem_open(argv[i], 0);
			if (semaphore == SEM_FAILED) {
				perror(argv[i]);
				return 1;
			}
			if (sem_close(semaphore) != 0) {
				perror(argv[i]);
				return 1;
			}
		} else if (unlink_semaphore(argv[i]) != 0) {
			return 1;
		}
	}
	return 0;
}
