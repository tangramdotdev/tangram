#!/usr/bin/env nu

# Remove the stale test resources: exited test database containers, dangling Docker images, unused Docker volumes, and leftover test temporary directories.
def main [] {
	# Remove the Docker resources if the Docker daemon is available.
	let docker = (docker info --format '{{.ServerVersion}}' | complete).exit_code == 0
	if $docker {
		# Remove the exited test database containers.
		let containers = docker ps --all --filter status=exited --filter name=tangram_test_ --quiet | lines
		if ($containers | is-not-empty) {
			docker rm --force ...$containers
		}

		# Remove the dangling images and the unused volumes.
		docker image prune --force
		docker volume prune --all --force
	} else {
		print 'the Docker daemon is not available; skipping the Docker resources'
	}

	# Remove the test temporary directories unless the cloud databases are running, which hold their state and database pool in them.
	if (databases_running) {
		print 'the cloud databases are running; keeping the test temporary directories'
	} else {
		let temp = $nu.temp-dir? | default $nu.temp-path?
		let directories = glob ($temp | path join 'tangram_test_*')
		for directory in $directories {
			rm --force --recursive $directory
		}
		print $'removed ($directories | length) test temporary directories'
	}
}

# Determine whether the cloud databases are running by connecting to the NATS port.
def databases_running [] {
	(bash -c 'exec 3<>/dev/tcp/127.0.0.1/4222' | complete).exit_code == 0
}
