use ../../test.nu *

# Pushing a nonexistent process and pulling a nonexistent process each fail, under both eager and lazy strategies.

def test_push [...args] {
	# Create a remote server.
	let remote = spawn --cloud --name remote

	# Create a local server.
	let local = spawn --name local

	# Add the remote.
	tg remote put default $remote.url

	# Try to push a nonexistent process.
	let fake_process_id = "pcs_0000000000000000000000000000"
	let output = tg push ...$args $fake_process_id | complete
	failure $output "pushing a nonexistent process should fail"
}

def test_pull [...args] {
	# Create a remote server.
	let remote = spawn --cloud --name remote

	# Create a local server.
	let local = spawn --name local

	# Add the remote.
	tg remote put default $remote.url

	# Try to pull a nonexistent process from the remote.
	let fake_process_id = "pcs_0000000000000000000000000000"
	let output = tg pull ...$args $fake_process_id | complete
	print --stderr $output
	failure $output
}

test_push "--eager"
test_push "--lazy"
test_pull "--eager"
test_pull "--lazy"
