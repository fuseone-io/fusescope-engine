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
{{- $_ := required "secretName: the Secret with FS_CATALOG_URL, FS_BUCKET_ACCESS_KEY and FS_BUCKET_SECRET_KEY" .Values.secretName -}}
{{- $_ := required "bucket.name" .Values.bucket.name -}}
{{- if .Values.tls.enabled -}}
{{- $_ := required "tls.secretName: the Secret with tls.crt, tls.key, public-ca.crt and hot-ca.crt" .Values.tls.secretName -}}
{{- end -}}
{{- if eq .Values.auth.mode "jwt" -}}
{{- if not .Values.tls.enabled -}}
{{- fail "auth.mode=jwt requires tls.enabled: a bearer JWT never crosses the network in plain text" -}}
{{- end -}}
{{- $_ := required "auth.jwksConfigMap: the ConfigMap with jwks.json" .Values.auth.jwksConfigMap -}}
{{- $_ := required "auth.issuer" .Values.auth.issuer -}}
{{- else if ne .Values.auth.mode "token" -}}
{{- fail (printf "auth.mode must be jwt or token, not %q" .Values.auth.mode) -}}
{{- end -}}
{{- end -}}

{{/* The pod spec every role shares; .role names it, .extra adds volumes. */}}
{{- define "fse.pod" -}}
{{- $v := .Values -}}
{{- $role := index $v .role -}}
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
    args: ["--config", "/etc/fusescope/config.toml", "--role", {{ .role | quote }}]
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
          name: {{ $v.secretName }}
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
        subPath: config.toml
        readOnly: true
      - name: spill
        mountPath: /var/lib/fusescope/spill
      {{- if eq .role "ingester" }}
      - name: wal
        mountPath: /var/lib/fusescope/wal
      {{- end }}
      {{- if $v.tls.enabled }}
      - name: tls
        mountPath: /etc/fusescope/tls
        readOnly: true
      {{- end }}
      {{- if eq $v.auth.mode "jwt" }}
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
  {{- if $v.tls.enabled }}
  - name: tls
    secret:
      secretName: {{ $v.tls.secretName }}
  {{- end }}
  {{- if eq $v.auth.mode "jwt" }}
  - name: jwt
    configMap:
      name: {{ $v.auth.jwksConfigMap }}
  {{- end }}
{{- end -}}
