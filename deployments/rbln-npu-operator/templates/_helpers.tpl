{{/*
Expand the name of the chart.
*/}}
{{- define "rbln-npu-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "rbln-npu-operator.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "rbln-npu-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "rbln-npu-operator.labels" -}}
helm.sh/chart: {{ include "rbln-npu-operator.chart" . }}
{{ include "rbln-npu-operator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "rbln-npu-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rbln-npu-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Operator image reference (shared by the controller Deployment and the CRD hook).
*/}}
{{- define "rbln-npu-operator.operatorImage" -}}
{{- printf "%s/%s:%s" (default "docker.io" .Values.operator.image.registry) .Values.operator.image.repository (.Values.operator.image.tag | default .Chart.AppVersion) -}}
{{- end }}

{{/*
RBLNDriver spec body from a driver-values dict (the driver block or a merged
instance). nodeSelector is intentionally rendered from the dict as-is: base
uses driver.nodeSelector, instances always carry their own.
Keep the field list in sync with RBLNDriverSpec (api/v1alpha1) and the driver
block in values.yaml — a field missing here is silently dropped from every
rendered CR. Also keep it in sync with the $allowed list in rblndriver.yaml's
instances guard (that one fails loudly on drift, so this comment is the only
place that needs remembering).
*/}}
{{- define "rbln-npu-operator.driverSpec" -}}
registry: {{ .image.registry | quote }}
image: {{ .image.repository | quote }}
version: {{ .image.tag | quote }}
imagePullPolicy: {{ .image.pullPolicy | quote }}
{{- with .imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .annotations }}
annotations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- if .priorityClassName }}
priorityClassName: {{ .priorityClassName | quote }}
{{- end }}
{{- with .resources }}
resources:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .env }}
env:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .manager }}
manager:
  {{- if .image }}
  registry: {{ .image.registry | quote }}
  image: {{ .image.repository | quote }}
  version: {{ .image.tag | quote }}
  imagePullPolicy: {{ .image.pullPolicy | quote }}
  {{- end }}
  {{- with .imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
{{- /* smd carries no tag: its image tag is always the driver version above. */}}
{{- with .smd }}
smd:
  {{- if .image }}
  registry: {{ .image.registry | quote }}
  image: {{ .image.repository | quote }}
  {{- end }}
{{- end }}
{{- end -}}
