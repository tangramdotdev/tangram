use ../../test.nu *

# A push takes time exponential in the depth of the object graph, not its size. The check that
# decides when the push may end clones its visited set at every edge, so it visits each node once per
# path that reaches it rather than once. The pull side answers the same question from flags on the
# roots.

# A chain of k directories over a distinct leaf, each naming the two directories below it. The chain
# gains one node per level and exponentially many paths.
def chain [k: int, leaf: string] {
	mut a = (tg put ('tg.file("' + $leaf + '")') | str trim)
	mut b = (tg put ('tg.directory({"z": ' + $a + '})') | str trim)
	for _ in 0..<$k {
		let c = (tg put ('tg.directory({"p": ' + $b + ', "q": ' + $a + '})') | str trim)
		$a = $b
		$b = $c
	}
	$b
}

# Create a remote server.
let remote = spawn --cloud --name remote

# Create a local server.
let local = spawn --name local

# Add the remote.
success (tg remote put default $remote.url | complete)

# The two chains share no objects, so pushing one does not let the other short circuit on a subtree
# that the remote already stores. Each holds its levels plus the leaf and its blob.
let shallow_k = 22
let deep_k = 30
let shallow = chain $shallow_k "shallow"
let deep = chain $deep_k "deep"

let shallow_elapsed = (timeit { tg push $shallow })
let deep_elapsed = (timeit { tg push $deep })

print $'shallow: ($shallow_k + 3) objects, ($shallow_elapsed)'
print $'deep:    ($deep_k + 3) objects, ($deep_elapsed)'
print $'ratio:   ($deep_elapsed / $shallow_elapsed)'

# The deeper chain holds eight more objects, so its push should cost about the same.
assert ($deep_elapsed < ($shallow_elapsed * 4))
