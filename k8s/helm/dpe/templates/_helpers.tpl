{{/*
Common labels. app.kubernetes.io/name is the Service name, and it is the label everything selects
on: Services, NetworkPolicies, the PodDisruptionBudget, and Prometheus's `service` target label.
*/}}
{{- define "dpe.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: dpe
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end }}

{{- define "dpe.selector" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{- define "dpe.image" -}}
{{ .root.Values.image.repository }}/{{ .name }}:{{ required "image.tag is required (k8s/up.sh sets it to the git commit; never `latest`)" .root.Values.image.tag }}
{{- end }}

{{/*
Configurations the chart knows to be broken, refused at render time rather than discovered at
runtime. Each is an arithmetic or a pairing that no single value can be wrong about on its own.
*/}}
{{- define "dpe.checks" -}}
{{- $orch := index .Values.services "payment-orchestrator" -}}
{{- if and (gt (int $orch.replicas) 1) (not .Values.auth.existingSecret) -}}
{{- fail (printf "services.payment-orchestrator.replicas is %d but auth.existingSecret is empty: each replica would generate its own signing key, and a token minted by one would be rejected by the other - intermittently, by load-balancer luck. Configure a key pair (k8s/up.sh creates one) or run one replica." (int $orch.replicas)) -}}
{{- end -}}
{{- if not .Values.prometheus.alertRules -}}
{{- fail "prometheus.alertRules is empty - pass --set-file prometheus.alertRules=infra/prometheus/alerts.yml. A Prometheus with no rules is a dashboard, and nothing would say so." -}}
{{- end -}}
{{- include "dpe.checkConnectionBudget" . -}}
{{- end }}

{{/*
Every pool, times its replicas PLUS ONE: a rolling update runs a surge pod (maxSurge 1) beside the
old ones, and every Deployment can be mid-rollout at once. Postgres refuses connection
max_connections+1 with "sorry, too many clients already", which a pool reports as a timeout on
some unrelated request - so the sum is checked here, where it is arithmetic.
*/}}
{{- define "dpe.checkConnectionBudget" -}}
{{- $total := 0 -}}
{{- $parts := list -}}
{{- range $name, $s := .Values.services -}}
{{- $n := mul (int $s.dbPoolSize) (add (int $s.replicas) 1) -}}
{{- $total = add $total $n -}}
{{- $parts = append $parts (printf "%s %d x (%d+1)" $name (int $s.dbPoolSize) (int $s.replicas)) -}}
{{- end -}}
{{- $budget := sub (int .Values.infra.postgresMaxConnections) (int .Values.infra.postgresReservedConnections) -}}
{{- if gt (int $total) (int $budget) -}}
{{- fail (printf "connection budget exceeded: %s = %d, but Postgres allows %d after the reserve (%d - %d). Lower a pool or a replica count, or raise max_connections first." (join " + " $parts) (int $total) (int $budget) (int .Values.infra.postgresMaxConnections) (int .Values.infra.postgresReservedConnections)) -}}
{{- end -}}
{{- end }}
