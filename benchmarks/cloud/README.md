# Run the benchmarks on a fixed cloud machine

This runner provisions one fixed cloud machine, runs the Sirannon and PostgreSQL suite on it, copies the results back, and deletes the machine. Every published number comes from the same named instance, so anyone can repeat the run and get the same result.

## Run it

```bash
PROVIDER=gcp ./run-cloud.sh all --yes --teardown
```

This creates the VM, pushes your working tree including uncommitted changes, installs the pinned toolchain (PostgreSQL 17 from the PGDG apt repository, Node 24 from NodeSource, pnpm, and Python 3), mounts the local NVMe for database data, runs the `cloud` preset (10,000,000 rows across both durability levels), copies each run directory into `benchmarks/server/results/runs/`, and deletes the VM on exit. Drop `--teardown` to keep the VM for inspection, and drop `--yes` to confirm before billing starts.

The suite measures Sirannon and PostgreSQL 17 as native processes in transient systemd units: the engine under test is pinned to its own CPU cores under a hard memory ceiling (cgroup v2), the Node load driver runs on disjoint cores, and each engine's data directory is proven at device level to be on the local NVMe before anything seeds. Each engine is driven through the client it provides: Sirannon over its SDK, PostgreSQL over its socket.

## Providers

Select the cloud with `PROVIDER` (default `gcp`). Each default is a dedicated-vCPU machine with about 8 vCPU, 32 GB, and local NVMe for the database, because shared vCPU tiers change speed under load and network-attached disks are too slow for full-durability seeding.

| `PROVIDER` | CLI | Machine (~8 vCPU / 32 GB) | Local NVMe | Login |
| --- | --- | --- | --- | --- |
| `gcp` | `gcloud` | `c3-standard-8-lssd` (Sapphire Rapids) | bundled, 2×375 GB | managed |
| `hetzner` | `hcloud` | `ccx33` (dedicated EPYC) | root disk | `root` |
| `digitalocean` | `doctl` | `gd-8vcpu-32gb` (Premium) | root disk | `root` |
| `aws` | `aws` | `m6id.2xlarge` (Ice Lake) | bundled, 474 GB | `ubuntu` |

AWS runs on Ice Lake because no current x86 general-purpose instance pairs Sapphire Rapids with a local NVMe instance store; each result records its CPU. Check each provider's own pricing page for the current rate.

## Prerequisites

- Authenticate the provider's CLI: run `gcloud auth login` and set a project, `hcloud context create` or export `HCLOUD_TOKEN`, `doctl auth init`, or `aws configure`.
- Create an SSH key pair for Hetzner, DigitalOcean, and AWS. The toolkit uses `~/.ssh/id_ed25519` or `~/.ssh/id_rsa`, and `SSH_KEY` points it at another. GCP manages its own keys.

## Dry run

`--dry-run` prints every command the toolkit would run without touching the cloud:

```bash
PROVIDER=aws ./run-cloud.sh all --dry-run
```

For a real end-to-end check on a small VM, shrink the seed and the windows, then tear it down:

```bash
PROVIDER=hetzner BENCH_DATA_SIZE=10000 BENCH_WORKLOADS=point-select BENCH_TARGET_RATES=1000 \
  BENCH_RUNS=2 BENCH_WARMUP_SECONDS=1 BENCH_MEASURE_SECONDS=2 \
  ./run-cloud.sh all --yes --teardown
```

## Individual steps

`all` runs `up`, `sync`, `setup`, `run`, and `fetch` in order. Run any one on its own while iterating:

```bash
PROVIDER=gcp ./run-cloud.sh up      # create the VM
PROVIDER=gcp ./run-cloud.sh sync    # re-push the working tree after a change
PROVIDER=gcp ./run-cloud.sh run     # rebuild and re-run on the existing VM
PROVIDER=gcp ./run-cloud.sh logs    # re-attach after an SSH drop
PROVIDER=gcp ./run-cloud.sh fetch   # pull run directories back
PROVIDER=gcp ./run-cloud.sh ssh     # open a shell on the VM
PROVIDER=gcp ./run-cloud.sh down    # delete the VM
```

`run` launches the benchmark detached, so it survives an SSH drop or a lost connection. Re-attach any time with `logs`.

## Configuration

Every default is an environment variable:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROVIDER` | `gcp` | `gcp`, `hetzner`, `digitalocean`, or `aws` |
| `VM_NAME` | `sirannon-bench` | Instance name |
| `MACHINE_TYPE` | per provider | Instance size |
| `DISK_SIZE` | `60` | Boot disk size in GB (ignored by Hetzner, which bundles storage) |
| `SSH_KEY` | `~/.ssh/id_ed25519` | Private key for the raw-SSH providers |
| `BENCH_PROFILE` | `cloud` | Run profile: `cloud` (10,000,000 rows) or `smoke` (quick check) |
| `BENCH_MACHINE_LABEL` | derived | Host label recorded in results |
| `BENCH_DURABILITIES` | `full matched` | Durability levels to run, space separated |
| `BENCH_WORKLOADS` | per config | Workloads to run, comma separated |
| `BENCH_TARGET_RATES` | per config | Offered request rates for the sweep, comma separated |
| `BENCH_DATA_SIZE` | `10000` | Seed row count per workload |
| `BENCH_RUNS` | `5` | Independent measured runs per rate |
| `BENCH_WARMUP_SECONDS` | `3` | Discarded warmup window |
| `BENCH_MEASURE_SECONDS` | `10` | Measurement window |

Provider-specific variables: `GCP_PROJECT`, `GCP_ZONE`, `MIN_CPU_PLATFORM`, and `USE_IAP` for GCP; `HCLOUD_LOCATION` for Hetzner; `DO_REGION` for DigitalOcean; and `AWS_REGION`, `AWS_SUBNET`, `AWS_AMI`, and `AWS_SG_NAME` for AWS. The toolkit forwards every `BENCH_*` variable to the VM unchanged.

## What each provider sets up

- **GCP** connects through `gcloud`, so it manages keys and opens no ports. Set `USE_IAP=1` to reach the VM through IAP with no public IP.
- **Hetzner** and **DigitalOcean** register your public key, create the server, and connect as `root` over its public IP.
- **AWS** imports your key pair, creates a security group that allows SSH only from your current public IP, resolves the latest Ubuntu 24.04 AMI from Canonical, tags the instance by name, and connects as `ubuntu`. It assumes a default VPC with a default subnet; set `AWS_SUBNET` if you have neither.

## Security

Hetzner and DigitalOcean expose port 22 on a public IP, so treat the VM as throwaway and always tear it down. AWS restricts port 22 to your own IP through the security group it creates. GCP with `USE_IAP=1` opens no public IP at all.

## Results

Each fetched run is a self-describing directory under `results/runs/<id>/` that records the machine, the commit, the durability settings, and both engine versions. The toolkit copies runs back but never commits them. Commit the run you want to publish, then run `python3 benchmarks/writeup/generate.py` to regenerate the page.
