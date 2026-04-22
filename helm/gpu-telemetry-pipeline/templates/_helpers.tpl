{{/*
Expand the name of the chart.
*/}}
{{- define "gpu-telemetry-pipeline.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gpu-telemetry-pipeline.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label.
*/}}
{{- define "gpu-telemetry-pipeline.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "gpu-telemetry-pipeline.labels" -}}
helm.sh/chart: {{ include "gpu-telemetry-pipeline.chart" . }}
app.kubernetes.io/name: {{ include "gpu-telemetry-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels – stable subset used in Deployment.spec.selector.
*/}}
{{- define "gpu-telemetry-pipeline.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gpu-telemetry-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Build the full image reference for a service.

Accepts a dict with keys:
  registry    – per-service registry (may be empty)
  repository  – image repository (required)
  tag         – resolved image tag (required)
*/}}
{{- define "gpu-telemetry-pipeline.image" -}}
{{- if .registry }}
{{- printf "%s/%s:%s" .registry .repository .tag }}
{{- else }}
{{- printf "%s:%s" .repository .tag }}
{{- end }}
{{- end }}

{{/*
Resolve the image for a service using per-service overrides with global fallbacks.
Call with: (dict "svc" .Values.<service> "root" $)
*/}}
{{- define "gpu-telemetry-pipeline.serviceImage" -}}
{{- $reg := .svc.image.registry | default .root.Values.global.imageRegistry }}
{{- $tag := .svc.image.tag | default .root.Values.image.tag }}
{{- include "gpu-telemetry-pipeline.image" (dict "registry" $reg "repository" .svc.image.repository "tag" $tag) }}
{{- end }}

{{/*
PostgreSQL hostname – always derived from fullname so it matches the Service name.
*/}}
{{- define "gpu-telemetry-pipeline.postgresHost" -}}
{{- printf "%s-postgresql" (include "gpu-telemetry-pipeline.fullname" .) }}
{{- end }}

{{/*
Full DATABASE_URL used by collector and api-gateway.
*/}}
{{- define "gpu-telemetry-pipeline.databaseURL" -}}
{{- if .Values.db.external.enabled }}
{{- .Values.db.external.url }}
{{- else }}
{{- printf "postgres://%s:%s@%s:5432/%s?sslmode=disable"
      .Values.postgresql.auth.username
      .Values.postgresql.auth.password
      (include "gpu-telemetry-pipeline.postgresHost" .)
      .Values.postgresql.auth.database }}
{{- end }}
{{- end }}

{{/*
MQ Server base URL – always derived from fullname so it matches the Service name.
*/}}
{{- define "gpu-telemetry-pipeline.mqURL" -}}
{{- printf "http://%s-mq-server:%d" (include "gpu-telemetry-pipeline.fullname" .) (.Values.mqServer.service.port | int) }}
{{- end }}

{{/*
imagePullSecrets list.
*/}}
{{- define "gpu-telemetry-pipeline.imagePullSecrets" -}}
{{- with .Values.global.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
