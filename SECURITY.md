# Security Policy

The current portfolio build reads explicitly supplied local CSV paths and loads a local SQLite warehouse. A hosted production service should never allow untrusted users to select arbitrary server-side filesystem paths. Production hardening should use authenticated source connectors or object-storage references, tenant isolation, secrets management, encrypted storage, upload scanning, authorization and structured audit logs.
