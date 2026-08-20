# Start the local cloud services in a Kubernetes context.
def main [context?: string] {
	let context_args = if $context == null { [] } else { ["--context", $context] }
	kubectl ...$context_args apply -f scripts/cloud/kubernetes.yaml
	kubectl ...$context_args apply -f scripts/cloud/observability.yaml
	kubectl ...$context_args wait --for=condition=ready pod -l app=fdb --timeout=60s
	kubectl ...$context_args wait --for=condition=ready pod -l app=nats --timeout=60s
	kubectl ...$context_args wait --for=condition=ready pod -l app=postgres --timeout=60s
	kubectl ...$context_args wait --for=condition=ready pod -l app=scylla --timeout=120s
	kubectl ...$context_args wait --namespace observability --for=condition=available deployment/clickstack --timeout=300s
	kubectl ...$context_args wait --namespace observability --for=condition=available deployment/otel-region --timeout=120s
	kubectl ...$context_args rollout status --namespace observability daemonset/otel-node --timeout=120s
	kubectl ...$context_args wait --for=condition=available deployment/fdb-exporter --timeout=120s
	kubectl ...$context_args wait --for=condition=available deployment/nats-exporter --timeout=120s
	print "ClickStack: http://localhost:8080"
}
