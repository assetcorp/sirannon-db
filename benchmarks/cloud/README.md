# Run the benchmarks on a fixed cloud machine

This runner provisions two fixed cloud machines on one private network, runs the Sirannon and PostgreSQL suite with the engines on one machine and the load generator on the other, copies the results back, and deletes both machines. Every published number comes from the same named instances, so anyone can repeat the run.

## Run it

```bash
PROVIDER=gcp ./run-cloud.sh all --yes --teardown
```

This creates the engine VM and the load VM, pushes your working tree including uncommitted changes to both, installs the pinned toolchain (PostgreSQL 18 from the PGDG apt repository, Node 24 from NodeSource, pnpm, and Python 3) on the engine VM and Node, pnpm, and Python on the load VM, mounts the local NVMe for database data, gives the engine VM an SSH key that reaches the load VM, runs the `cloud` preset (10,000,000 rows across both durability levels), copies each run directory into `benchmarks/server/results/runs/`, and deletes both VMs on exit. Drop `--teardown` to keep the VMs for inspection, and drop `--yes` to confirm before billing starts.

The suite measures Sirannon and PostgreSQL 18 one after the other as native processes in transient systemd units on the engine VM. The engine under test gets every logical CPU of that VM under a hard memory ceiling (cgroup v2), and each engine's data directory is proven at device level to be on the local NVMe before anything seeds. The Node load generator runs on the load VM and reaches each engine over the private network through the client that the engine provides: Sirannon over its SDK, PostgreSQL over node-postgres. Each result records the network delay between the two VMs.

## Providers

Select the cloud with `PROVIDER` (default `gcp`). Each default is a dedicated-vCPU machine with about 8 vCPU, 32 GB, and local NVMe for the database, because shared vCPU tiers change speed under load and network-attached disks are too slow for full-durability seeding.

| `PROVIDER` | CLI | Machine (~8 vCPU / 32 GB) | Local NVMe | Login |
| --- | --- | --- | --- | --- |
| `gcp` | `gcloud` | `c3-standard-8-lssd` (Sapphire Rapids) | bundled, 2×375 GB | managed |
| `hetzner` | `hcloud` | `ccx33` (dedicated EPYC) | root disk | `root` |
| `digitalocean` | `doctl` | `gd-8vcpu-32gb` (Premium) | root disk | `root` |
| `aws` | `aws` | `m6id.2xlarge` (Ice Lake) | bundled, 474 GB | `ubuntu` |

AWS runs on Ice Lake because no current x86 general-purpose instance pairs Sapphire Rapids with a local NVMe instance store; each result records its CPU. Check each provider's own pricing page for the current rate.

The load VM needs no local NVMe, so GCP uses `c3-standard-8` and AWS uses `m6i.2xlarge`, which have the same CPU generation and size as the engine VM. Hetzner and DigitalOcean use the engine VM's type. `LOAD_MACHINE_TYPE` overrides the default, and the load VM's name is `VM_NAME-load` unless `LOAD_VM_NAME` is set.

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
PROVIDER=gcp ./run-cloud.sh up        # create both VMs and the network between them
PROVIDER=gcp ./run-cloud.sh sync      # re-push the working tree after a change
PROVIDER=gcp ./run-cloud.sh run       # rebuild and re-run on the existing VMs
PROVIDER=gcp ./run-cloud.sh logs      # re-attach after an SSH drop
PROVIDER=gcp ./run-cloud.sh fetch     # pull run directories back
PROVIDER=gcp ./run-cloud.sh ssh       # open a shell on the engine VM
PROVIDER=gcp ./run-cloud.sh ssh-load  # open a shell on the load VM
PROVIDER=gcp ./run-cloud.sh down      # delete both VMs and the network
```

`sync` replaces the checkout on the VM, but it carries every run directory under `results/runs/` and `results/.smoke/runs/` on the VM into the new checkout. Where a file that you push and a file on the VM share a path, the VM's copy is the one that `sync` keeps, as `fetch` does, so a run that you haven't fetched yet survives a sync, and a pass that you repeat into an existing run adds to that run.

`run` launches the benchmark detached, so it survives an SSH drop or a lost connection. Re-attach any time with `logs`.

## Configuration

Every default is an environment variable:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROVIDER` | `gcp` | `gcp`, `hetzner`, `digitalocean`, or `aws` |
| `VM_NAME` | `sirannon-bench` | Instance name |
| `MACHINE_TYPE` | per provider | Engine VM size |
| `LOAD_MACHINE_TYPE` | per provider | Load VM size |
| `LOAD_VM_NAME` | `VM_NAME-load` | Load VM name |
| `DISK_SIZE` | `60` | Boot disk size in GB (ignored by Hetzner, which bundles storage) |
| `VM_MAX_HOURS` | `24` | Self-delete backstop in hours; `0` disables it (GCP and AWS only) |
| `SSH_KEY` | `~/.ssh/id_ed25519` | Private key for the raw-SSH providers |
| `BENCH_PROFILE` | `cloud` | Run profile: `cloud` (10,000,000 rows) or `smoke` (quick check) |
| `BENCH_MACHINE_LABEL` | derived | Engine VM label recorded in results |
| `BENCH_LOAD_MACHINE_LABEL` | derived | Load VM label recorded in results |
| `BENCH_CONCURRENCY_LEVELS` | `16,64,256` | Limits on the requests in flight, comma separated |
| `BENCH_DURABILITIES` | `full matched` | Durability levels to run, space separated |
| `BENCH_ENGINES` | `sirannon postgres` | Engines to run, space separated |
| `BENCH_RUN_ID` | timestamp | Run directory to write into; set it to an existing run id to add a pass to that run |
| `BENCH_WORKLOADS` | per config | Workloads to run, comma separated |
| `BENCH_TARGET_RATES` | per config | Offered request rates for the sweep, comma separated |
| `BENCH_DATA_SIZE` | `10000` | Seed row count per workload |
| `BENCH_RUNS` | `5` | Independent measured runs per rate |
| `BENCH_WARMUP_SECONDS` | `3` | Discarded warmup window |
| `BENCH_MEASURE_SECONDS` | `10` | Measurement window |

Provider-specific variables: `GCP_PROJECT`, `GCP_ZONE`, `GCP_NETWORK`, `MIN_CPU_PLATFORM`, and `USE_IAP` for GCP; `HCLOUD_LOCATION`, `HCLOUD_NETWORK`, and `HCLOUD_NETWORK_ZONE` for Hetzner; `DO_REGION` for DigitalOcean; and `AWS_REGION`, `AWS_SUBNET`, `AWS_AMI`, and `AWS_SG_NAME` for AWS. The toolkit forwards every `BENCH_*` variable to the VM unchanged.

## What each provider sets up

- **GCP** connects through `gcloud`, so it manages keys. It tags both VMs and adds a firewall rule, `VM_NAME-between-vms`, that opens TCP ports 22, 5432, 9876, and 9877 between the tagged VMs on `GCP_NETWORK` (default `default`). Set `USE_IAP=1` to reach the VMs through IAP with no public IP. A network other than `default` needs its own rule that allows TCP port 22 from your machine, or from `35.235.240.0/20` with `USE_IAP=1`, since [Google's IAP TCP forwarding](https://docs.cloud.google.com/iap/docs/using-tcp-forwarding) connects from that range.
- **Hetzner** registers your public key, creates a private network `VM_NAME-net` on 10.77.0.0/16, attaches both servers to it, and connects as `root` over each server's public IP. `down` deletes the network after the servers.
- **DigitalOcean** registers your public key, creates both droplets in the region's default VPC, which connects them privately, and connects as `root` over each droplet's public IP.
- **AWS** imports your key pair, creates a security group that allows SSH only from your current public IP and every TCP port between its own members, resolves the latest Ubuntu 24.04 AMI from Canonical, puts the load VM in the engine VM's subnet, tags the instances by name, and connects as `ubuntu`. It assumes a default VPC with a default subnet; set `AWS_SUBNET` if you have neither.

## Security

Hetzner and DigitalOcean expose port 22 on a public IP, so treat the VM as throwaway and always tear it down. AWS restricts port 22 to your own IP through the security group it creates. GCP with `USE_IAP=1` opens no public IP at all.

## Self-delete backstop

The `down` command and `--teardown` only run while `run-cloud.sh` itself is alive, so a machine that dies mid-run would leave both VMs billing forever. `VM_MAX_HOURS` (default 24) caps that for each VM: on GCP the instance is created with a termination time and the DELETE action, so Google's control plane deletes it, its boot disk, and its local SSDs at the deadline even if the guest is hung; on AWS the instance schedules an in-guest halt and terminates on shutdown, which deletes the root volume. Hetzner and DigitalOcean have no equivalent, and the toolkit says so at create time.

The 24-hour default clears the worst case where every timeout in a `cloud` run is fully exhausted (about 22 hours including provisioning, since the harness measures each workload at three in-flight limits). `run` refuses to start when the remaining lifetime of either VM is shorter than the profile could need, and `up` prints the exact deadline. Anything not fetched by the deadline is deleted with the VM, so after losing your connection to a run, fetch results before the printed time. Set `VM_MAX_HOURS=0` to disable the backstop, and rely on `down` alone.

## Results

Each fetched run is a self-describing directory under `results/runs/<id>/` that records the machine, the commit, the durability settings, and both engine versions. The toolkit copies runs back but never commits them. Commit the run you want to publish, then run `python3 benchmarks/writeup/generate.py` to regenerate the page.
