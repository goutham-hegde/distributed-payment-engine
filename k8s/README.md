# Kubernetes (M10)

The same system on a one-node [kind](https://kind.sigs.k8s.io/) cluster: a Helm chart for the three
services, the console and a pod-discovering Prometheus, plus plain manifests for the stateful tier.

```
k8s/
├── kind/cluster.yaml         one node; localhost:8084 (console) and :9090 (Prometheus)
├── infra/                    Postgres, Redpanda, Redis + their NetworkPolicies  (namespace dpe-infra)
├── helm/dpe/                 the application chart                               (namespace dpe)
├── up.sh                     cluster, images, infra, signing key, chart - idempotent
├── verify-netpol.sh          proves the NetworkPolicies are enforced, not just stored
└── rollout-under-load.sh     restarts every Deployment under load; judges from the databases
```

The design and its reasoning are in [ADR 0013](../docs/adr/0013-kubernetes.md).

## Run it

Needs Docker, `kind`, `kubectl`, `helm` and `openssl` on PATH (tested with kind 0.32 / Kubernetes
1.36.1, Helm 4.2). The cluster and the Compose stack publish the same host ports, so stop Compose
first:

```bash
docker compose -f infra/docker-compose.yml stop
./k8s/up.sh                      # ~5 min cold: builds the four images, loads them into the node
./k8s/verify-netpol.sh           # 0 = every forbidden connection was refused
./k8s/rollout-under-load.sh      # ~6 min; 0 = nothing lost and every k6 threshold met
DISRUPT=kill ./k8s/rollout-under-load.sh    # SIGKILL an orchestrator instead: money must hold,
                                            # completions are reported, not asserted
```

`DISRUPT=kill` kills the JVM from the node (`kill -9` of the container's host pid), because
`kubectl delete pod --force --grace-period=0` is not a crash: the container is still signalled, its
consumers leave the group cleanly, and the scenario passes for the wrong reason. Each run writes
`k8s/results/<run>/` (k6 log and summary, accounts, restart timings).

Every command names the context. `up.sh` puts back whatever your current context was, because
`kind create cluster` switches it - so a bare `kubectl` here may be talking to another cluster:

```bash
kubectl --context kind-dpe -n dpe get pods
```

The invariant scripts take a command prefix for reaching Postgres:

```bash
PG_EXEC="kubectl --context kind-dpe -n dpe-infra exec -i postgres-0 --" ./scripts/verify-invariants.sh
```

Stop without losing data: `docker stop dpe-control-plane` (then `./k8s/up.sh` starts it again).
Delete the cluster, ledger included: `kind delete cluster --name dpe`.

## What differs from Compose, and why

| | Compose | Here |
|---|---|---|
| Management port boundary | unpublished port | NetworkPolicy (default deny ingress), proven by `verify-netpol.sh` |
| Health | `wget /actuator/health` (includes Redis, DB) | liveness/readiness **groups** only - a dependency outage must not restart pods |
| Signing key | generated at start | `dpe-signing-key` Secret, made by `up.sh` - required for 2 orchestrators |
| Scrape targets | three static names | pod discovery; a Service would load-balance the scrapes |
| Shutdown | SIGTERM, 30 s grace | `preStop` sleep 5 s, then SIGTERM, inside the same 30 s |
| Memory | `limits.memory` 768M | request = limit = 768Mi; no CPU limit (ADR 0012's budget) |
| Orchestrator | 1 | 2 (HTTP and relay capacity - see ADR 0013 for what it does *not* add) |
| Tracing | Jaeger | off (sample rate 0) - RAM; set `tracing.otlpEndpoint` to turn on |
| Chaos suite | `chaos/` | not ported - it drives containers with `docker` |
