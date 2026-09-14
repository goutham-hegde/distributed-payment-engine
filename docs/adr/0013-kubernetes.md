# ADR 0013 — Kubernetes: putting back what Compose was doing for free

**Status:** accepted (M10)
**Related:** ADR 0005 (the signing key), ADR 0009 (admission control), ADR 0012 (the memory budget),
ADR 0006 (tracing must not be load-bearing)

## Context

M0–M9 ran on Docker Compose: one copy of each container on one machine. Several decisions made
since then lean on Compose without saying so, and a Kubernetes deployment removes each of them
quietly - nothing fails to start, and the system is subtly wrong:

- The management port (M6) answers `/actuator/**` with `permitAll`, justified by the port being
  unpublished: "that network boundary *is* the scrape credential". Kubernetes' pod network is flat -
  every pod can open every port of every other pod.
- The orchestrator generates its RSA signing key at startup (ADR 0005). Two replicas generate two.
- Prometheus scrapes three static names. With replicas, a name is a Service, and a Service
  load-balances.
- Four gauges (`dpe_outbox_backlog`, `dpe_outbox_age_seconds`, `dpe_dlq_depth`, `dpe_saga_inflight`)
  count a *table*, and every replica counts the same table. Dashboards and alerts `sum` them.
- A stop was SIGTERM then 30 s. Kubernetes sends SIGTERM *at the same moment* it starts removing
  the pod from Service endpoints.

The deliverable is a claim that can be tested: **a rolling deploy of every service, under load,
loses nothing** - no failed request, no failed payment, I1–I5 and S1–S4 green.

## Decisions

### 1. The stateful tier is not in the chart

`k8s/helm/dpe` deploys the three services, the console and Prometheus, and takes Postgres, the
broker and Redis as **addresses**. `k8s/infra/` holds plain manifests for a laptop (single-replica
StatefulSets, local-path volumes). In production those addresses point at managed services and the
chart does not change; and `helm uninstall` must never be able to delete the ledger. The Postgres
init script and the Redpanda bootstrap file are mounted from ConfigMaps **created from the Compose
files**, not copied.

### 2. Probes ask about the process, not its dependencies

All three on the management port. Startup (`/actuator/health/liveness`, up to 3 min - Boot, Flyway
and `AlwaysPreTouch` on a shared node), liveness (`/actuator/health/liveness`) and readiness
(`/actuator/health/readiness`) - Boot's *groups*, which contain only the application's own state.
Never the full `/actuator/health`, which includes the Redis and database indicators:

- as liveness, a Redis outage restarts every orchestrator - **Redis load-bearing through a probe**,
  breaking the rule that deleting Redis may only make the system slower; and a database blip
  crash-loops every pod at once, all returning together when it comes back;
- as readiness, a database outage marks every replica unready and empties the Service, so callers
  get connection refused instead of a 503 that says what is wrong;
- readiness also must not include "holds Kafka partitions": with three partitions per topic and
  three consumers per replica, a replica can legitimately hold none and would never become ready.

### 3. The signing key is a Secret made outside the chart; the chart refuses the broken case

`k8s/up.sh` generates `dpe-signing-key` once (`openssl`, PKCS#8/X.509 DER) and never replaces it.
Helm's `genPrivateKey` was rejected: it runs on every `helm upgrade`, so every deploy would rotate
the key and invalidate every token in flight. The chart **fails to render** more than one
orchestrator replica without a configured key.

Measured, by going around that guard (`kubectl scale` does not run the chart): two orchestrators
with generated keys, ten logins of four requests each. The orchestrator answered **19 of 40 with
401**; account-service, which verifies through a JWKS fetched and cached via the Service, answered
**20 of 40 with 401** - split by login, because it had cached one pod's key. Nothing was
misconfigured on any single pod. It also shows the limit of a chart-level guard: it binds `helm`,
not `kubectl` or an autoscaler.

### 4. Prometheus discovers pods, and shared facts are aggregated with `max`

`kubernetes_sd_configs` (role `pod`, this namespace only, a namespaced `Role` - not a
`ClusterRole`), keeping the container port named `management`; `instance` is the pod name. Scraping
the Service would land each scrape on a random replica, and one series would alternate between two
pods' counters.

Three rule and query changes follow, all of which also run under Compose:

- **Table-counting gauges use `max by (application)`** in `alerts.yml`, the Grafana dashboard, the
  console's System view and the load report. With two orchestrators, `sum` reported double: a
  backlog alert at 1000 would have fired at 500. Counters (`dpe_saga_terminal_total`) are still
  summed - each pod counts its own work.
- **`KafkaConsumerFetchSpin` groups by `instance` as well as `client_id`.** A client id is unique
  only within one JVM; two replicas run `consumer-payment-orchestrator-1` each, and grouping by id
  alone adds a healthy replica's consumption to a stalled replica's spin, so `== 0` is never true.
- **`ServiceHasNoScrapeTargets`** (`absent(up{service=...})`), a Kubernetes-only rule in the chart.
  Under Compose a dead service is a static target that stops answering and `TargetDown` fires; with
  pod discovery a Deployment with no pods has *no* targets, `up == 0` never becomes true, and every
  rule over it goes silent.

### 5. NetworkPolicy puts the management-port boundary back, and a test proves it is enforced

Default-deny ingress in both namespaces, then one allowance per real arrow: console → the three API
ports; account-service and gateway → the orchestrator's API port (JWKS); pods labelled
`dpe/api-client` (k6) → API ports; Prometheus → the three management ports; anyone → the two
NodePorts; the app namespace → Postgres, Redpanda, Redis. Services talk through Kafka, not HTTP,
which is why the list is short. Ingress only; default-deny egress is the next step and is not done.

The API server stores a NetworkPolicy whether or not the network plugin enforces it, so
`k8s/verify-netpol.sh` pairs every BLOCKED expectation with an ALLOWED one on the same port (the
port is open to the right client and closed to the wrong one). 10 of 10 held; with the app
namespace's policies deleted, 4 failed - the management port was reachable **from a pod in another
namespace**, which is what "permitAll on an unpublished port" means on a flat network. kindnet
enforces policies and still admits the kubelet's probes under default-deny.

A label is not an identity - anything that can create pods here can label one `dpe/api-client`.
That is acceptable only because the API ports still demand a signed token.

### 6. Resources come from ADR 0012; memory request = limit; no CPU limit

768Mi, from the budget table (heap 256 + metaspace 176 + code cache 144 + direct 32 + native ~140).
Request = limit for memory: with `-Xms = -Xmx` and `AlwaysPreTouch` the whole heap is resident from
the first second, and a lower request lets the scheduler overcommit the node. **No CPU limit**: a
CFS quota freezes a GC that spends it until the next 100 ms period (ADR 0012's *Real ≫ User+Sys*
signature), and since JDK 19 the JVM sizes `availableProcessors` from the limit alone. The cost is
Burstable QoS rather than Guaranteed.

### 7. A rolling deploy: surge first, preStop sleep, then graceful shutdown

`maxSurge: 1, maxUnavailable: 0`; `preStop: sleep 5`; `terminationGracePeriodSeconds: 30` (the
countdown includes the sleep); a PodDisruptionBudget only for a Deployment with more than one
replica (with one, `minAvailable: 1` blocks every node drain).

### 8. The connection budget is checked when the chart renders

Every pool × (replicas + 1 surge pod) must fit in `max_connections` minus a reserve, or `helm
template` fails with the arithmetic (4 orchestrators: 12×5 + 10×2 + 10×2 = 100 > 87). The pool size
reaches the service as `SPRING_DATASOURCE_HIKARI_MAXIMUMPOOLSIZE`, so the number checked is the
number used - verified by setting account-service to 11 and reading `hikaricp_connections_max` = 11.

### 9. Two orchestrators, one gateway, and what a replica does not add

Admission control is **not** divided by the replica count: it counts in-flight sagas from
`saga_instances`, so every replica already enforces the same global limit (ADR 0009); N replicas
overshoot by at most N × one count interval's admissions. A second orchestrator adds HTTP and relay
capacity. It does not move the knee, which is the simulated PSP. Consumers beyond partitions idle:
three partitions per topic, three reply consumers per replica - the range assignor gave one pod
two reply partitions and the other one. The gateway stays at one replica: its simulation knobs are
held in the pod's memory, so through a Service `POST /admin/simulation` would configure whichever
replica answered.

### 10. Hardening that costs nothing

`runAsNonRoot` with a numeric UID (the image's `USER app` is a name the kubelet cannot verify),
read-only root filesystem with an `emptyDir` at `/tmp` (GC log, Tomcat's work directory),
`capabilities: drop [ALL]`, `automountServiceAccountToken: false` - these services never call the
Kubernetes API, so a mounted token is a credential only an attacker would use.

### 11. The saga deadline outlasts an orphaned partition: step-timeout 60 s

Run D survived by timing (see Consequences), so run E removed the luck: SIGSTOP instead of SIGKILL.
A frozen process sends no heartbeat and no LeaveGroup and does not restart until the liveness probe
gives up, so nothing triggers an early rebalance - the survivor keeps its partitions, its readiness
gate stays open, and its sweeper keeps running while the frozen member's partitions sit orphaned
for the 45 s session timeout (47 s measured). At step-timeout 30 s, **13 healthy payments were
failed**: every one had its `FundsReserved` published within a second of acceptance, and was timed
out ~30 s later with that reply still unread. Money stayed correct (I1–I5, S1–S4) - the
compensations commute (M7) - but customers were told "failed" about payments that had worked.

The choice was between lowering `session.timeout.ms` below the step timeout and raising the step
timeout above it. **The step timeout went to 60 s** (45 s session + rebalance + margin); run F, the
same freeze, failed nothing. The cost is that a payment whose participant is genuinely dead is
compensated after 60 s rather than 30, with its money held that much longer; the alternative's cost
- a consumer evicted after ~10 s of silence - lands on every long pause of a healthy one. The
chaos harness's `SAGA_DEADLINE` now carries the number, and scenarios 01 and 02 derive their
"shorter than" and "past" the deadline timings from it.

## Results

`k8s/rollout-under-load.sh`: the M8 k6 script in-cluster (20 customers, 1–2 s think, ~4.6 payments/s)
against the Service - not through `kubectl port-forward`, which pins one pod and dies with it -
judged from `saga_instances` after quiescence, plus I1–I5 and S1–S4. One run each, except A:

| Run | Disruption | Failed requests | Sagas COMPLETED | Timed out | Settle p99 (DB) | Invariants |
|---|---|---|---|---|---|---|
| A | rolling restart ×3 (orchestrator 57 s, account 27 s, gateway 26 s) | **0 / 6,874** | 1,298 / 1,298 | 0 | 3.13 s | hold |
| A′ | A again, on a cluster recreated from nothing (77 / 32 / 36 s) | **0 / 6,847** | 1,310 / 1,310 | 0 | 3.29 s | hold |
| B | A without the preStop sleep | 4 / 6,834 (2 creates) | 1,317 / 1,317 | 0 | 3.12 s | hold |
| C | `kubectl delete pod --force --grace-period=0` | 0 / 6,850 | 1,357 / 1,357 | 0 | 3.11 s | hold |
| D | SIGKILL of the JVM from the node | 2 / 7,343 (0 creates) | 1,139 / 1,139 | 0 | **42.5 s** | hold |
| E | SIGSTOP of the JVM (a hung process), step-timeout 30 s | 9 / 6,279 | **1,053 / 1,066** | **13** | 31.0 s | hold |
| F | E again after decision 11, step-timeout 60 s | 0 / 6,138 | 1,050 / 1,050 | 0 | 39.0 s | hold |

**B** is the endpoint race, measured: all four errors were `connect: connection refused` to the
Service's ClusterIP at the two instants an old orchestrator received SIGTERM. The sleep closes it.

**C is not a crash**, and the first version of the kill scenario used it. The pod object is deleted
at once, but the container is still stopped with a signal: the dead pod's partitions were handed
over in **4 s**, which under the classic group protocol is only possible if its consumers sent
LeaveGroup - the JVM ran its shutdown. A fault that is quietly a graceful stop makes the scenario
pass (the same lesson as M7's unauthenticated fault injector). **D** is the crash: `kill -9` of the
container's host PID from the node, which is what an OOMKill does. Its partitions stayed assigned
to the dead members for **46 s** - `session.timeout.ms`, 45 s.

## Consequences

- **A crashed orchestrator stalls the whole group, not just its partitions.** In run D the crashed
  container restarted in place ~20 s after the kill and rejoined as new members; under the eager range
  assignor that join started a rebalance, the surviving pod revoked *all* its partitions, and the
  coordinator waited for the dead members' sessions to expire before completing it. For ~23 s no
  reply was consumed anywhere.
- **No payment was wrongly compensated in run D, and the reason is timing, not design.** The
  survivor could sweep for 24 s after the kill, and the first stuck saga's deadline was at 30 s;
  once the survivor's partitions were revoked, Fix E's gate (M7) paused its sweeper. Had the
  restart taken longer than ~28 s - a pod rescheduled onto another node, a slow start - the
  survivor would have timed out healthy sagas whose replies sat on the orphaned partitions. Fix E
  covers "*this* instance cannot hear"; two replicas add "*a sibling's* partitions are orphaned",
  which it does not. `session.timeout.ms` (45 s) must be brought under the saga's `step-timeout`
  (30 s), or the step timeout raised above it - a timing budget like the relay's (18 s < 30 s).
  Runs E and F settled it, and decision 11 records the choice.
- A second deployment description to keep in step with Compose: the JVM flags are repeated in
  `values.yaml` (the budget table stays in `docker-compose.yml`), and the Service names must match
  the console's `nginx.conf`.
- RAM. The node used 3.8 GiB at rest and 4.4 GiB mid-rollout; with Windows and WSL, free host
  memory fell to 2.0 GB. It cannot run beside the Compose stack, and they share host ports on
  purpose so that trying fails loudly.
- One node is one failure domain. This proves the manifests; it does not prove availability.
- The chaos suite (`chaos/`) is not ported - it drives containers with `docker`. Tracing is off in
  the cluster (sample rate 0, to save RAM); `tracing.otlpEndpoint` turns it on.

## Alternatives rejected

- **The stateful tier as chart dependencies** (third-party Postgres/Kafka charts). A second
  configuration surface with its own defaults, for components this milestone is not about, and it
  puts the ledger's lifetime under `helm uninstall`.
- **prometheus-operator and `ServiceMonitor`.** CRDs plus an operator pod to express one scrape
  config; the pod-discovery config above is the same idea without either.
- **An autoscaler (HPA) for the orchestrator.** Admission control would survive it (the bound is
  global), but the chart's connection-budget and signing-key guards would not - an autoscaler is
  `kubectl scale` on a timer. And the knee is downstream at the PSP, so more replicas buy no
  throughput.
- **An Ingress controller.** A NodePort suffices on kind; an ingress-nginx pod adds memory and
  nothing this milestone needs to prove.
- **`kind load docker-image`.** It imports `--all-platforms`, and a multi-platform image pulled into
  Docker Desktop's containerd store holds only this machine's platform, so the import failed
  (`ctr: content digest ... not found`). `up.sh` exports the one platform with
  `docker save --platform` and imports that.
