{{- define "fse.name" -}}
{{- .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- end -}}

{{- define "fse.labels" -}}
app.kubernetes.io/name: fusescope-engine
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "fse.selector" -}}
app.kubernetes.io/name: fusescope-engine
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .role }}
{{- end -}}

{{/* Refuses a release that would run half-configured or insecurely. */}}
{{- define "fse.validate" -}}
{{- $v := .Values -}}
{{- $needed := list "ingester" "querier" "compactor" -}}
{{- if $v.migration.enabled -}}
{{- $needed = append $needed "migrator" -}}
{{- end -}}
{{- $names := list -}}
{{- range $role := $needed -}}
{{- $names = append $names (required (printf "secrets.%s: the Secret with FS_CATALOG_URL and, for the roles, the bucket credentials" $role) (index $v.secrets $role)) -}}
{{- end -}}
{{- if $v.tls.enabled -}}
{{- $names = concat $names (list $v.tls.clientCASecret $v.tls.identities.ingester $v.tls.identities.querier) -}}
{{- end -}}
{{- if ne (len $names) (len (uniq $names)) -}}
{{- fail (printf "every Secret must differ: each role's environment, the migrator's, the client CAs and each identity, since a shared one hands a role another's credentials or key: %v" $names) -}}
{{- end -}}
{{- if and $v.migration.enabled (or (lt (int64 $v.migration.backoffLimit) 0) (gt (int64 $v.migration.backoffLimit) 10)) -}}
{{- fail (printf "migration.backoffLimit must be between 0 and 10, not %v" $v.migration.backoffLimit) -}}
{{- end -}}
{{- if or (lt (int $v.startupTimeoutSecs) 1) (gt (int $v.startupTimeoutSecs) 86400) -}}
{{- fail (printf "startupTimeoutSecs must be between 1 and 86400, not %v" $v.startupTimeoutSecs) -}}
{{- end -}}
{{- $_ := required "bucket.name" $v.bucket.name -}}
{{- if $v.tls.enabled -}}
{{- $_ := required "tls.clientCASecret: the Secret with public-ca.crt and hot-ca.crt" $v.tls.clientCASecret -}}
{{- $_ := required "tls.identities.ingester: the ingester's Secret with tls.crt and tls.key" $v.tls.identities.ingester -}}
{{- $_ := required "tls.identities.querier: the querier's Secret with tls.crt and tls.key" $v.tls.identities.querier -}}
{{- end -}}
{{- if eq $v.auth.mode "jwt" -}}
{{- if not $v.tls.enabled -}}
{{- fail "auth.mode=jwt requires tls.enabled: a bearer JWT never crosses the network in plain text" -}}
{{- end -}}
{{- $_ := required "auth.jwksConfigMap: the ConfigMap with jwks.json" $v.auth.jwksConfigMap -}}
{{- $_ := required "auth.issuer" $v.auth.issuer -}}
{{- else if ne $v.auth.mode "token" -}}
{{- fail (printf "auth.mode must be jwt or token, not %q" $v.auth.mode) -}}
{{- end -}}
{{- end -}}

{{/* Whether .role uses TLS: the compactor serves and dials no Flight. */}}
{{- define "fse.tls" -}}
{{- if and .Values.tls.enabled (ne .role "compactor") }}true{{ end -}}
{{- end -}}

{{/* Whether .role verifies JWTs: only the querier serves Flight SQL. */}}
{{- define "fse.jwt" -}}
{{- if and (eq .Values.auth.mode "jwt") (eq .role "querier") }}true{{ end -}}
{{- end -}}

{{/* The configuration of .role. */}}
{{- define "fse.config" -}}
{{- $v := .Values -}}
# Rendered by the chart for the {{ .role }} role. Secrets come from the
# environment, the node id from the pod name.
[server]
role = {{ .role | quote }}
flight_addr = "0.0.0.0:8815"
metrics_addr = "0.0.0.0:9464"
drain_timeout_secs = {{ $v.drainTimeoutSecs }}

[catalog]
max_connections = {{ $v.catalog.maxConnections }}
# The migration Job migrates; a role's user has no DDL.
migrate = false

[bucket]
kind = "s3"
name = {{ $v.bucket.name | quote }}
{{- with $v.bucket.prefix }}
prefix = {{ . | quote }}
{{- end }}
{{- with $v.bucket.endpoint }}
endpoint = {{ . | quote }}
{{- end }}
{{- with $v.bucket.region }}
region = {{ . | quote }}
{{- end }}
allow_http = {{ $v.bucket.allowHttp }}

[wal]
dir = "/var/lib/fusescope/wal"

[ingest]
listen = "0.0.0.0:4317"
hot_listen = "0.0.0.0:8816"

[query]
spill_dir = "/var/lib/fusescope/spill"

[tls]
{{- if include "fse.tls" . }}
enabled = true
cert = "/etc/fusescope/tls/tls.crt"
key = "/etc/fusescope/tls/tls.key"
public_client_ca = "/etc/fusescope/ca/public-ca.crt"
hot_client_ca = "/etc/fusescope/ca/hot-ca.crt"
reload_interval_secs = {{ $v.tls.reloadIntervalSecs }}
{{- else }}
enabled = false
{{- end }}

[auth]
{{- if include "fse.jwt" . }}
mode = "jwt"
jwt_keys = "/etc/fusescope/jwt/jwks.json"
jwt_issuer = {{ $v.auth.issuer | quote }}
{{- else }}
mode = "token"
{{- end }}

[log]
level = {{ $v.log.level | quote }}
format = {{ $v.log.format | quote }}
{{- end -}}

{{/* The pod spec of .role, from a dict of Values, Release, Chart and role. */}}
{{- define "fse.pod" -}}
{{- $v := .Values -}}
{{- $role := index $v .role -}}
serviceAccountName: {{ include "fse.name" . }}-{{ .role }}
automountServiceAccountToken: false
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile:
    type: RuntimeDefault
terminationGracePeriodSeconds: {{ add $v.drainTimeoutSecs 30 }}
{{- with $v.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $role.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $role.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $role.affinity }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
containers:
  - name: engine
    image: "{{ $v.image.repository }}:{{ $v.image.tag | default .Chart.AppVersion }}"
    imagePullPolicy: {{ $v.image.pullPolicy }}
    args: ["--config", "/etc/fusescope/config.toml"]
    env:
      - name: POD_NAME
        valueFrom:
          fieldRef:
            fieldPath: metadata.name
      - name: FS_NODE_ID
        value: "$(POD_NAME)"
      {{- if eq .role "ingester" }}
      - name: FS_INGEST_HOT_ADVERTISE
        value: "$(POD_NAME).{{ include "fse.name" . }}-ingester-hot.{{ .Release.Namespace }}.svc:8816"
      {{- end }}
    envFrom:
      - secretRef:
          name: {{ index $v.secrets .role }}
    ports:
      {{- if eq .role "ingester" }}
      - name: otlp
        containerPort: 4317
      - name: hot
        containerPort: 8816
      {{- end }}
      {{- if eq .role "querier" }}
      - name: flight
        containerPort: 8815
      {{- end }}
      - name: metrics
        containerPort: 9464
    # /healthz answers once the start is done: bucket, catalog, migrations
    # and the prefix's ownership. Liveness and readiness wait for it.
    startupProbe:
      httpGet:
        path: /healthz
        port: metrics
      periodSeconds: 5
      failureThreshold: {{ div (add $v.startupTimeoutSecs 4) 5 }}
    livenessProbe:
      httpGet:
        path: /healthz
        port: metrics
      periodSeconds: 10
      failureThreshold: 6
    readinessProbe:
      httpGet:
        path: /readyz
        port: metrics
      periodSeconds: 5
    securityContext:
      readOnlyRootFilesystem: true
      allowPrivilegeEscalation: false
      capabilities:
        drop: ["ALL"]
    {{- with $role.resources }}
    resources:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    volumeMounts:
      - name: config
        mountPath: /etc/fusescope/config.toml
        subPath: {{ .role }}.toml
        readOnly: true
      - name: spill
        mountPath: /var/lib/fusescope/spill
      {{- if eq .role "ingester" }}
      - name: wal
        mountPath: /var/lib/fusescope/wal
      {{- end }}
      {{- if include "fse.tls" . }}
      - name: tls
        mountPath: /etc/fusescope/tls
        readOnly: true
      - name: client-ca
        mountPath: /etc/fusescope/ca
        readOnly: true
      {{- end }}
      {{- if include "fse.jwt" . }}
      - name: jwt
        mountPath: /etc/fusescope/jwt
        readOnly: true
      {{- end }}
volumes:
  - name: config
    configMap:
      name: {{ include "fse.name" . }}-config
  - name: spill
    emptyDir:
      {{- if eq .role "querier" }}
      sizeLimit: {{ $v.querier.spillSizeLimit }}
      {{- else }}
      sizeLimit: 1Gi
      {{- end }}
  {{- if include "fse.tls" . }}
  - name: tls
    secret:
      secretName: {{ index $v.tls.identities .role }}
  - name: client-ca
    secret:
      secretName: {{ $v.tls.clientCASecret }}
  {{- end }}
  {{- if include "fse.jwt" . }}
  - name: jwt
    configMap:
      name: {{ $v.auth.jwksConfigMap }}
  {{- end }}
{{- end -}}

{{/* Pod template annotations of .role: its configuration and rollout token. */}}
{{- define "fse.podAnnotations" -}}
checksum/config: {{ include "fse.config" . | sha256sum }}
{{- with index .Values.rolloutTokens .role }}
fusescope.io/rollout-token: {{ . | quote }}
{{- end }}
{{- end -}}
