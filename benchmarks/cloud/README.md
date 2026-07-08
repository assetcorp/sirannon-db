# Run the benchmarks on a disclosed cloud machine

Correctness numbers reproduce on any machine because the workloads are seeded and
deterministic. Performance numbers (throughput, latency) only reproduce on one
fixed, named machine, since a laptop thermally throttles under sustained load,
cannot pin its CPU frequency, and shares the machine with everything else. This
toolkit provisions that machine on the cloud of your choice, runs the Docker
benchmark track on it, copies the results back, and deletes it, so a published run
always comes from the same disclosed hardware and anyone can repeat it.

One shared core drives every provider. Each provider is a small driver under
`providers/` that supplies create, ssh, scp, and delete; everything else (git
packaging, the detached run, result fetching, dry-run, teardown) lives in
`lib/common.sh` and is identical everywhere.

## Pick a provider

Select the cloud with the `PROVIDER` variable (default `gcp`). Every provider
defaults to a dedicated-vCPU machine with about 8 vCPU and 32 GB, because shared
tiers wander under load and ruin reproducible timing.

| `PROVIDER` | CLI | Machine (dedicated, ~8 vCPU / 32 GB) | Login | ~$/hr |
| --- | --- | --- | --- | --- |
| `gcp` | `gcloud` | `c3-standard-8` (Sapphire Rapids) | managed | ~$0.40 |
| `hetzner` | `hcloud` | `ccx33` (dedicated EPYC) | `root` | ~$0.25 |
| `digitalocean` | `doctl` | `g-8vcpu-32gb` | `root` | ~$0.24 |
| `aws` | `aws` | `m7i.2xlarge` (Sapphire Rapids) | `ubuntu` | ~$0.40 |

Prices are approximate on-demand rates; confirm on each provider's pricing page.
Hetzner and DigitalOcean run about 40% cheaper than GCP and AWS.

## Prerequisites

- The provider's CLI, authenticated: `gcloud auth login` and a project set,
  `hcloud context create` or `HCLOUD_TOKEN`, `doctl auth init`, or `aws configure`.
- For `hetzner`, `digitalocean`, and `aws`, an SSH key pair. The toolkit uses
  `~/.ssh/id_ed25519` or `~/.ssh/id_rsa` by default; set `SSH_KEY=/path/to/key`
  to choose another. The public key is `<key>.pub`. GCP manages its own keys, so
  it needs none of this.

## One command

```bash
PROVIDER=hetzner ./run-cloud.sh all
```

That creates the VM, pushes your current working tree (including uncommitted
changes), installs Docker, Node 22, pnpm, and k6, builds the workspace, runs the
engine and e2e Docker suites, and copies each result file into
`packages/ts/benchmarks/results/`. It leaves the VM running so you can inspect it,
and prints the command to delete it. Add `--teardown` to delete on success and
`--yes` to skip the billing confirmation:

```bash
PROVIDER=hetzner ./run-cloud.sh all --yes --teardown
```

Both suites run Sirannon and Postgres 17 in resource-capped Docker containers, so
the comparison is fair on both sides. The containers build from `node:22-trixie-slim`
as pinned in `../docker/`, so Sirannon runs on Node 22 regardless of the host.

## Test it before you trust it

`--dry-run` prints every command the toolkit would run on any provider, touching
nothing and spending nothing:

```bash
PROVIDER=aws ./run-cloud.sh all --dry-run
```

For an end-to-end check against a real VM for pennies, restrict the work to the
engine suite and one small workload, then tear down afterwards. `SUITES=engine`
skips the k6 e2e half, and `BENCH_DATA_SIZES` and `BENCH_WORKLOADS` limit the
engine suite:

```bash
PROVIDER=hetzner SUITES=engine BENCH_DATA_SIZES=1000 BENCH_WORKLOADS=point-select \
  ./run-cloud.sh all --yes --teardown
```

## Steps you can run on their own

`all` is `up`, `sync`, `setup`, `run`, and `fetch` in order. Run any alone while
iterating:

```bash
PROVIDER=hetzner ./run-cloud.sh up      # create the VM
PROVIDER=hetzner ./run-cloud.sh sync    # re-push the working tree after a change
PROVIDER=hetzner ./run-cloud.sh run     # rebuild and re-run on the existing VM
PROVIDER=hetzner ./run-cloud.sh logs    # re-attach after an SSH drop
PROVIDER=hetzner ./run-cloud.sh fetch   # pull result files back
PROVIDER=hetzner ./run-cloud.sh ssh     # open a shell on the VM
PROVIDER=hetzner ./run-cloud.sh down    # delete the VM
```

`run` launches the work detached on the VM, so closing your laptop or losing the
connection does not stop it. Re-attach any time with `logs`.

## Configuration

Every default is an environment variable:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PROVIDER` | `gcp` | `gcp`, `hetzner`, `digitalocean`, or `aws` |
| `VM_NAME` | `sirannon-bench` | Instance name |
| `MACHINE_TYPE` | per provider | Instance size |
| `DISK_SIZE` | `60` | Boot disk size in GB (ignored by Hetzner, which bundles storage) |
| `SUITES` | `both` | `both`, `engine`, or `e2e` |
| `SSH_KEY` | `~/.ssh/id_ed25519` | Private key for the raw-SSH providers |
| `BENCH_MACHINE_LABEL` | derived | Host label recorded in results |
| `BENCH_DURABILITY` | `matched` | `matched` or `full` WAL-flush strategy for both engines |
| `BENCH_DATA_SIZES` | per script | Engine-suite row counts (comma separated) |
| `BENCH_WORKLOADS` | per script | Engine-suite workloads (comma separated) |
| `BENCH_DATA_SIZE` | `10000` | e2e-suite dataset size |
| `BENCH_RPS_LEVELS` | per script | e2e-suite target request rates |
| `BENCH_DURATION` | per script | e2e-suite k6 stage duration |

Provider-specific: `GCP_PROJECT`, `GCP_ZONE`, `MIN_CPU_PLATFORM`, `USE_IAP` for
GCP; `HCLOUD_LOCATION` for Hetzner; `DO_REGION` for DigitalOcean; `AWS_REGION`,
`AWS_SUBNET`, `AWS_AMI`, `AWS_SG_NAME` for AWS. Every `BENCH_*` variable set on
the control host is forwarded to the VM unchanged.

## What each provider sets up

- **GCP** uses `gcloud` for the connection, so it manages keys and needs no open
  ports. Set `USE_IAP=1` to reach the VM through IAP with no public IP at all.
- **Hetzner** and **DigitalOcean** register your public key, create the server,
  and connect as `root` over its public IP.
- **AWS** imports your key pair, creates a security group that allows SSH only
  from this machine's current public IP, resolves the latest Ubuntu 24.04 AMI
  from Canonical, tags the instance by name, and connects as `ubuntu`. It assumes
  a default VPC with a default subnet; set `AWS_SUBNET` if you have neither.

## Security

The raw-SSH providers expose port 22 on a public IP. AWS restricts that to your
own IP through the security group it creates. Hetzner and DigitalOcean leave 22
reachable, so treat the VM as throwaway and always tear it down. GCP with
`USE_IAP=1` is the most locked-down option, with no public IP.

## Publishing a run

Fetched result files carry the machine, CPU, memory, Node, SQLite, and Postgres
versions in their own `system` block, so a committed run is self-describing. The
toolkit copies files back but never commits them; commit the timestamped JSON and
CSV you want to publish. The raw per-run k6 summaries stay out of git by the
existing ignore rules.
