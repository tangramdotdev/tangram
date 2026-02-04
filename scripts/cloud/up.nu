kubectl apply -f scripts/cloud/kubernetes.yaml
kubectl wait --for=condition=ready pod -l app=postgres --timeout=60s
kubectl wait --for=condition=ready pod -l app=scylla --timeout=120s
kubectl wait --for=condition=ready pod -l app=nats --timeout=60s
kubectl wait --for=condition=ready pod -l app=fdb --timeout=60s
kubectl wait --for=condition=ready pod -l app=alloy --timeout=60s
kubectl wait --for=condition=ready pod -l app=tempo --timeout=60s
kubectl wait --for=condition=ready pod -l app=loki --timeout=60s
kubectl wait --for=condition=ready pod -l app=prometheus --timeout=60s
kubectl wait --for=condition=ready pod -l app=grafana --timeout=60s
