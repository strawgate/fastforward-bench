# memagent-e2e (Performance & Integrity Lab)

Combined laboratory for performance benchmarking and service-level integrity testing of the `memagent` log collector.

## Focus Areas

- **Performance & Scale**: EPS "ladder" and "max" tests to establish performance curves and ceilings.
- **Service Integrity**: End-to-end functional validation using real services (Redis, Nginx, Memcached) and synthetic edge cases (Rotation, Multiline).
- **Competitive Intelligence**: Comparative runs against `otelcol`, `vector`, and `filebeat`.
- **Resource Profiling**: Deep analysis of CPU and Memory efficiency (Cores per 100k EPS).

## Workflows

The repository organizes tests into environment-specific suites:

1. **Cloud-Native Scalability (K8s)**
   - **File**: `.github/workflows/bench-kind-smoke.yml`
   - **Goal**: Measure scalability and stability in a Kubernetes cluster.
2. **Local Throughput (Docker)**
   - **File**: `.github/workflows/bench-compose-smoke.yml`
   - **Goal**: Measure raw collector efficiency and Max EPS in Docker.
3. **Core Service Integrity (Smoke)**
   - **File**: `.github/workflows/e2e-smoke.yml`
   - **Goal**: Fast functional validation of core services and parsers.
4. **Full Service Integrity (Nightly)**
   - **File**: `.github/workflows/e2e-nightly.yml`
   - **Goal**: Comprehensive daily validation of all supported scenarios.

## Shortcuts (Manual Trigger)

Use **Workflow Dispatch** in the Actions tab for quick runs:

- **Logfwd Max EPS**: In benchmark workflows, set `target_set` to `logfwd_max` to skip ladder steps and competitors. Ideal for rapid resource-efficiency validation.

## Reporting

Reports are posted to stable GitHub Issues which are updated in-place:

1. **Cloud-Native Scalability (K8s) Report**
2. **Local Throughput (Docker) Report**
3. **Core Service Integrity (Smoke) Report**
4. **Full Service Integrity (Nightly) Report**
5. **System Dashboard**: A high-level summary of all project test results.
