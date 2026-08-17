# Deploying on GCP for free

Runs the existing 3-node `docker-compose.yml` cluster on a single GCP
Always Free `e2-micro` VM. No trial credit needed, no time limit, no
charge, as long as you stay inside the Always Free limits below.

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

## Steps

### 1. Create the VM

Using Cloud Shell (browser-based, nothing to install) or a local `gcloud`
install:

```bash
gcloud compute instances create raftkv-demo \
  --zone=us-central1-a \
  --machine-type=e2-micro \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --boot-disk-size=30GB
```

### 2. Open a firewall rule for the CLI port

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

Drop `--source-ranges` to `0.0.0.0/0` instead if you want it reachable from
anywhere, understanding that means anyone can write to it.

### 3. Install Docker and run the cluster

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
```

### 4. Test it

From your own machine, using the VM's external IP:

```bash
EXTERNAL_IP=$(gcloud compute instances describe raftkv-demo --zone=us-central1-a --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

go run ./client --peers node1=$EXTERNAL_IP:7001,node2=$EXTERNAL_IP:7002,node3=$EXTERNAL_IP:7003 put hello world
go run ./client --peers node1=$EXTERNAL_IP:7001,node2=$EXTERNAL_IP:7002,node3=$EXTERNAL_IP:7003 get hello
```

Run this from a genuinely separate machine, not the VM itself. GCP does not
route a VM's own traffic back in through its external IP, so testing from
inside the VM against its own public address just times out. If you're
testing from Cloud Shell instead of a local machine, remember its egress IP
can change between sessions, so re-check it against the firewall rule's
`--source-ranges` if a request that worked before suddenly stops connecting.

Or just SSH into the VM and use the CLI locally the same way the
docker-compose usage examples in the main README show.

### 5. Tear down

Deleting the instance stops billing for it immediately (not that there was
any, inside the free limits):

```bash
gcloud compute instances delete raftkv-demo --zone=us-central1-a
gcloud compute firewall-rules delete raftkv-demo
```
