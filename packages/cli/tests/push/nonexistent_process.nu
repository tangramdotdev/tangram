use ../../test.nu *

export def test_push [...args] {
	# Create a remote server.
	let remote = spawn -n remote

	# Create a local server.
	let local = spawn -n local

	# Add the remote.
	tg remote put default $remote.url

	# Try to push a nonexistent process.
	let fake_process_id = "pcs_0000000000000000000000000000"
	let output = tg push ...$args $fake_process_id | complete
	failure $output "pushing a nonexistent process should fail"
}

export def test_pull [...args] {
	# Create a remote server.
	let remote = spawn -n remote

	# Create a local server.
	let local = spawn -n local

	# Add the remote.
	tg remote put default $remote.url

	# Try to pull a nonexistent process from the remote.
	let fake_process_id = "pcs_0000000000000000000000000000"
	let output = tg pull ...$args $fake_process_id | complete
	print -e $output
	failure $output
}
