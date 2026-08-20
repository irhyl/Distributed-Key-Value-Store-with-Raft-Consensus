# Deploying on GCP

Runs the existing 3-node `docker-compose.yml` cluster on a single GCP
Always Free `e2-micro` VM. No trial credit needed, no time limit, no
charge, as long as you stay inside the Always Free limits below. Verified
end to end: cluster deployed, reached from a separate external machine,
and torn down cleanly with zero cost.

## Always Free limits that apply here

- One `e2-micro` instance per billing account, in `us-west1`, `us-central1`,
  or `us-east1` only. Pick a different region and it stops being free.
- 30GB standard persistent disk.
- 1GB/month network egress to most destinations (not counting traffic to
  other GCP services or within the same region).

This deploys all 3 Raft nodes as containers on the one free VM. It proves
the software runs on GCP and gives you a live link, but it does not
demonstrate real multi-machine fault tolerance, since all 3 nodes share the
same underlying hardware. For that you'd want one VM per node, which falls
outside the Always Free tier.

## Billing account, before anything else

GCP requires an open billing account with a payment method attached before
it will create any Compute Engine resource, even ones that end up costing
$0. If this is a new account, the $300/90-day trial credit covers this
automatically. If the trial has expired, its billing account gets closed
and can't be reused - you need to open a new, regular (non-trial) billing
account instead. You don't need the trial credit for this guide; the
`e2-micro` VM is free under Always Free regardless of trial status.

```bash
gcloud billing accounts list
```

Look for one with `OPEN: True`. If none exist, add one at
[console.cloud.google.com/billing](https://console.cloud.google.com/billing).
Adding a card does not mean charges are automatic beyond what you actually
use - but GCP billing is postpaid, so if you exceed Always Free limits the
card on file does get charged with no extra confirmation step. Set a low
budget alert right after adding the account as a tripwire:

Billing console -> Budgets & alerts -> Create budget, scope it to your
project, set the amount to something small (₹100 or $1), keep the default
alert thresholds. This only sends a notification, it does not stop
spending - but it means you find out within minutes if something runs
past the free limits.

## Steps

### 1. Create the project and link billing

```bash
gcloud projects create raftkv-demo-$RANDOM --name="raftkv-demo"
gcloud config set project YOUR_PROJECT_ID
gcloud billing projects link YOUR_PROJECT_ID --billing-account=YOUR_BILLING_ACCOUNT_ID
gcloud services enable compute.googleapis.com
```

No spaces around `=` in `--billing-account=ID` - `--billing-account = ID`
gets parsed as three separate unrecognized arguments and fails.

### 2. Create the VM

```bash
gcloud compute instances create raftkv-demo \
  --zone=us-central1-a \
  --machine-type=e2-micro \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --boot-disk-size=30GB
```

### 3. Open a firewall rule for the CLI port

The KV store has no TLS and no authentication (see the README's "what's not
implemented" list), so anyone who can reach the port can read and write.
Restrict the source range to your own IP unless you want a fully public demo:

```bash
MY_IP=$(curl -s ifconfig.me)
gcloud compute firewall-rules create raftkv-demo \
  --allow=tcp:7001-7003 \
  --source-ranges="$MY_IP/32" \
  --target-tags=raftkv-demo
gcloud compute instances add-tags raftkv-demo --tags=raftkv-demo --zone=us-central1-a
```

Run `curl -s ifconfig.me` from whichever machine you'll actually test from,
not just wherever you happen to be running `gcloud` at the time - they're
not always the same machine, and the firewall rule only allows the IP it
was given. If you're testing from Cloud Shell, its egress IP can change
between sessions, so re-check it if a request that worked before suddenly
times out. Drop `--source-ranges` to `0.0.0.0/0` instead if you want it
reachable from anywhere, understanding that means anyone can write to it.

### 4. Install Docker and run the cluster

```bash
gcloud compute ssh raftkv-demo --zone=us-central1-a
```

Once connected:

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker

git clone https://github.com/<your-username>/Distributed-Key-Value-Store-with-Raft-Consensus.git
cd Distributed-Key-Value-Store-with-Raft-Consensus
docker compose up -d
docker compose ps
```

A brief round of election churn in the first second or two of startup is
normal - multiple nodes can start elections concurrently before one wins
cleanly. Check `docker compose logs` if you want to see it settle.

### 5. Test it

From a genuinely separate machine, not the VM itself:

```bash
EXTERNAL_IP=$(gcloud compute instances describe raftkv-demo --zone=us-central1-a --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

go run ./client --peers node1=$EXTERNAL_IP:7001,node2=$EXTERNAL_IP:7002,node3=$EXTERNAL_IP:7003 put hello world
go run ./client --peers node1=$EXTERNAL_IP:7001,node2=$EXTERNAL_IP:7002,node3=$EXTERNAL_IP:7003 get hello
```

GCP does not route a VM's own traffic back in through its external IP, so
testing from inside the VM against its own public address just times out -
this is not a firewall problem, it's expected behavior. Use Cloud Shell or
your own machine instead.

Or just SSH into the VM and use the CLI locally the same way the
docker-compose usage examples in the main README show.

### 6. Tear down

Deleting the instance stops billing for it immediately (not that there was
any, inside the free limits):

```bash
gcloud compute instances delete raftkv-demo --zone=us-central1-a
gcloud compute firewall-rules delete raftkv-demo
```

Confirm nothing's left:

```bash
gcloud compute instances list
gcloud compute disks list
```

Both should return nothing. Deleting the instance auto-deletes its boot
disk; a lingering disk is the one thing that could still accrue a small
storage charge if the delete didn't clean up properly.
