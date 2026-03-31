#!/usr/bin/env bash
set -euo pipefail

printf '%s' "$CDSYNC_GCP_KEY_B64" | base64 -d >/tmp/gcp-key.json

cat >/tmp/rds-ca.pem <<'EOF'
-----BEGIN CERTIFICATE-----
MIID/zCCAuegAwIBAgIRAPVSMfFitmM5PhmbaOFoGfUwDQYJKoZIhvcNAQELBQAw
gZcxCzAJBgNVBAYTAlVTMSIwIAYDVQQKDBlBbWF6b24gV2ViIFNlcnZpY2VzLCBJ
bmMuMRMwEQYDVQQLDApBbWF6b24gUkRTMQswCQYDVQQIDAJXQTEwMC4GA1UEAwwn
QW1hem9uIFJEUyB1cy1lYXN0LTEgUm9vdCBDQSBSU0EyMDQ4IEcxMRAwDgYDVQQH
DAdTZWF0dGxlMCAXDTIxMDUyNTIyMzQ1N1oYDzIwNjEwNTI1MjMzNDU3WjCBlzEL
MAkGA1UEBhMCVVMxIjAgBgNVBAoMGUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4x
EzARBgNVBAsMCkFtYXpvbiBSRFMxCzAJBgNVBAgMAldBMTAwLgYDVQQDDCdBbWF6
b24gUkRTIHVzLWVhc3QtMSBSb290IENBIFJTQTIwNDggRzExEDAOBgNVBAcMB1Nl
YXR0bGUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDu9H7TBeGoDzMr
dxN6H8COntJX4IR6dbyhnj5qMD4xl/IWvp50lt0VpmMd+z2PNZzx8RazeGC5IniV
5nrLg0AKWRQ2A/lGGXbUrGXCSe09brMQCxWBSIYe1WZZ1iU1IJ/6Bp4D2YEHpXrW
bPkOq5x3YPcsoitgm1Xh8ygz6vb7PsvJvPbvRMnkDg5IqEThapPjmKb8ZJWyEFEE
QRrkCIRueB1EqQtJw0fvP4PKDlCJAKBEs/y049FoOqYpT3pRy0WKqPhWve+hScMd
6obq8kxTFy1IHACjHc51nrGII5Bt76/MpTWhnJIJrCnq1/Uc3Qs8IVeb+sLaFC8K
DI69Sw6bAgMBAAGjQjBAMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYEFE7PCopt
lyOgtXX0Y1lObBUxuKaCMA4GA1UdDwEB/wQEAwIBhjANBgkqhkiG9w0BAQsFAAOC
AQEAFj+bX8gLmMNefr5jRJfHjrL3iuZCjf7YEZgn89pS4z8408mjj9z6Q5D1H7yS
jNETVV8QaJip1qyhh5gRzRaArgGAYvi2/r0zPsy+Tgf7v1KGL5Lh8NT8iCEGGXwF
g3Ir+Nl3e+9XUp0eyyzBIjHtjLBm6yy8rGk9p6OtFDQnKF5OxwbAgip42CD75r/q
p421maEDDvvRFR4D+99JZxgAYDBGqRRceUoe16qDzbMvlz0A9paCZFclxeftAxv6
QlR5rItMz/XdzpBJUpYhdzM0gCzAzdQuVO5tjJxmXhkSMcDP+8Q+Uv6FA9k2VpUV
E/O5jgpqUJJ2Hc/5rs9VkAPXeA==
-----END CERTIFICATE-----
EOF

cat >/tmp/config.yaml <<EOF
state:
  url: "postgres://cdsync_staging:${CDSYNC_DB_PASSWORD}@staging-postgres-private.c0qssstf2cvw.us-east-1.rds.amazonaws.com:5432/resq?sslmode=require"
  schema: "cdsync_state"

logging:
  level: "info"
  json: true

observability:
  service_name: "cdsync-staging-oneoff"
  metrics_interval_seconds: 30

sync:
  default_batch_size: 5000
  max_retries: 5
  retry_backoff_ms: 1000
  max_concurrency: 4

stats:
  url: "postgres://cdsync_staging:${CDSYNC_DB_PASSWORD}@staging-postgres-private.c0qssstf2cvw.us-east-1.rds.amazonaws.com:5432/resq?sslmode=require"
  schema: "cdsync_stats"

connections:
  - id: "staging_app"
    enabled: true
    source:
      type: postgres
      url: "postgres://cdsync_staging:${CDSYNC_DB_PASSWORD}@staging-postgres-private.c0qssstf2cvw.us-east-1.rds.amazonaws.com:5432/resq?sslmode=require"
      cdc: true
      publication: "cdsync_staging_pub"
      schema_changes: auto
      cdc_pipeline_id: 1101
      cdc_batch_size: 5000
      cdc_max_fill_ms: 2000
      cdc_max_pending_events: 100000
      cdc_idle_timeout_seconds: 10
      cdc_tls: true
      cdc_tls_ca_path: "/tmp/rds-ca.pem"
      tables:
        - name: "public.workorders_workorder"
          primary_key: "id"
          soft_delete: true
        - name: "public.workorders_workordercancellationdetail"
          primary_key: "id"
          soft_delete: true
        - name: "public.workorders_workorderreview"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_workordersummary"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_workorderclientinvoicesapproval"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_workorderdispute"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_workorderinvoicestrategy"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_clientinvoice"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_clientinvoicelineitem"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_clientinvoicetax"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_clientmanagedinvoice"
          primary_key: "id"
          soft_delete: true
        - name: "public.users_organization"
          primary_key: "id"
          soft_delete: true
        - name: "public.users_organization_tos_agreements"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_organizationfeatures"
          primary_key: "id"
          soft_delete: true
        - name: "public.core_organizationsubscription"
          primary_key: "id"
          soft_delete: true
    destination:
      type: bigquery
      project_id: "nora-461013"
      dataset: "cdsync_e2e_real"
      location: "US"
      service_account_key_path: "/tmp/gcp-key.json"
      partition_by_synced_at: true
      storage_write_enabled: false
      batch_load_bucket: "nora-461013-cdsync-staging-loads"
      batch_load_prefix: "staging-app"
EOF

cdsync validate --config /tmp/config.yaml --connection staging_app --verbose
cdsync sync --config /tmp/config.yaml --connection staging_app --incremental --schema-diff
cdsync report --config /tmp/config.yaml --connection staging_app --limit 10
