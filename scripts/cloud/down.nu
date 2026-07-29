# Stop the local cloud services in a Kubernetes context.
def main [context?: string] {
	let context_args = if $context == null { [] } else { ["--context", $context] }
	kubectl ...$context_args delete --ignore-not-found -f scripts/cloud/observability.yaml
	kubectl ...$context_args delete --ignore-not-found -f scripts/cloud/kubernetes.yaml
}
