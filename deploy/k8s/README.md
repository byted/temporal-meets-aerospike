# k3s deployment

A single-node k3s deployment of the demo: Temporal Server whose **operational
persistence can be switched between SQLite and Aerospike at runtime**, with a
small web control plane that does the switching.

The reference stack is [`deploy/docker-compose.yml`](../docker-compose.yml).
This reproduces it with two deliberate differences:

| | compose | k3s |
|---|---|---|
| Visibility | Elasticsearch | SQLite |
| Backend switch | not possible | `TEMPORAL_ENVIRONMENT` on the Deployment |

Elasticsearch is out of scope: it is a second stateful service with a JVM heap
on a ~4 GB box, and the demo is about the *operational* store.

```mermaid
flowchart LR
    user(["browser"])

    subgraph ns["namespace tma"]
        direction TB
        ing["Ingress (Traefik)<br/>/ → control-plane<br/>/temporal → UI"]
        cp["control-plane<br/>ServiceAccount: get/patch Deployment<br/>get/list Pods"]
        ui["temporal-ui :8080"]
        srv["temporal<br/>TEMPORAL_ENVIRONMENT=sqlite|aerospike<br/>:7233"]
        sq[("SQLite<br/>emptyDir")]
        as[("Aerospike EE 8.x<br/>SC namespace 'temporal'<br/>StatefulSet + PVC")]
        rinit(["roster-init<br/>sidecar"])
    end

    user --> ing --> cp
    ing --> ui --> srv
    cp -->|"patch env var"| srv
    srv -.->|"env=sqlite"| sq
    srv -.->|"env=aerospike"| as
    rinit -.->|"stage roster · revive · recluster"| as
```

## Why k3s and not Docker

The control plane is a web service that restarts a workload. On Docker that
means bind-mounting `/var/run/docker.sock`, which is root on the host for
anyone who gets past the basic auth. On Kubernetes it is a namespaced Role with
three verbs on two resource types — see
[`40-control-plane-rbac.yaml`](40-control-plane-rbac.yaml). That trade is the
reason this directory exists.

---

## 1. Install k3s

On a fresh amd64 Linux VM, ~4 GB RAM:

```sh
curl -sfL https://get.k3s.io | sh -

# So kubectl works as your user rather than through `sudo k3s kubectl`.
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown "$(id -u):$(id -g)" ~/.kube/config
kubectl get nodes
```

k3s brings its own Traefik ingress controller and a `local-path` default
StorageClass. Nothing else needs installing.

### 1b. …or k3d, to run the whole thing on a laptop

k3d is k3s inside Docker, so it is the same distribution, the same Traefik and
the same `local-path` provisioner as the box above — only the image-import verb
differs. This is the route to use on a Mac, and it is the one the manifests
were last verified against (macOS 15, arm64, Docker Desktop, k3d v5.9.0 /
k3s v1.35.5).

```sh
brew install k3d                       # kubectl too, if you do not have it

# -p maps the host port onto the cluster's Traefik. Without it the Ingress
# exists but nothing on the host can reach it.
k3d cluster create tma --agents 0 -p "8080:80@loadbalancer" --wait
```

Everything below is identical except step 2's import command, and the demo is
then at `http://localhost:8080/` rather than at the box's IP.

Give Docker Desktop ~6 GB. On the default 4 GB the cluster comes up, but a
concurrent `docker build` can starve the API server long enough that
`kubectl` reports `TLS handshake timeout` and the Traefik helm-install job
fails its first attempt — both recover on their own, so build the images
*before* creating the cluster if you would rather not watch that happen.

Tear down with `k3d cluster delete tma`.

**On an arm64 Mac, check `DOCKER_DEFAULT_PLATFORM` before building.** If it is
set to `linux/amd64` — a common workaround to have lying around in a shell
profile — both images build as amd64, import happily into an arm64 cluster and
then crash with `exec format error`. For a k3d cluster on Apple Silicon you
want arm64, so override it explicitly rather than trusting the default:

```sh
export DOCKER_DEFAULT_PLATFORM=linux/arm64      # or unset it
docker build --platform linux/arm64 -f deploy/Dockerfile -t ... .
docker image inspect temporal-meets-aerospike/server:dev --format '{{.Architecture}}'
```

## 2. Build and import the two images

Both images are built from source in this repo and are **never pushed to a
registry**, so every manifest sets `imagePullPolicy: IfNotPresent`.

Build on the demo box if you can — then the architecture matches by
construction:

```sh
# from the repo root
docker build -f deploy/Dockerfile               -t temporal-meets-aerospike/server:dev        .
docker build -f deploy/Dockerfile.control-plane -t temporal-meets-aerospike/control-plane:dev .

# containerd, not dockerd, runs the pods -- the image has to be handed over
sudo docker save temporal-meets-aerospike/server:dev        | sudo k3s ctr images import -
sudo docker save temporal-meets-aerospike/control-plane:dev | sudo k3s ctr images import -

sudo k3s ctr images ls | grep temporal-meets-aerospike
```

On **k3d**, the node is a container and k3d does the save-and-import itself —
one command replaces the three above:

```sh
k3d image import -c tma \
  temporal-meets-aerospike/server:dev \
  temporal-meets-aerospike/control-plane:dev

docker exec k3d-tma-server-0 crictl images | grep temporal-meets-aerospike
```

Building on an **arm64 Mac** for an amd64 box: an arm64 image imports happily
and then fails with `exec format error`, which does not look like an
architecture problem. Cross-build explicitly and copy the tarball over:

```sh
docker buildx build --platform linux/amd64 -f deploy/Dockerfile \
  -t temporal-meets-aerospike/server:dev --output type=docker,dest=server.tar .
scp server.tar demo-box:
ssh demo-box 'sudo k3s ctr images import server.tar'
```

The third-party images (Aerospike, aerospike-tools, temporalio/ui) are pulled
normally and are multi-arch. Nothing here pins a platform — the node decides.

## 3. Apply the manifests

Filenames are numbered and `kubectl apply -f <dir>` processes them in
lexical order, so the Namespace exists before anything lands in it:

```sh
kubectl apply -f deploy/k8s/
kubectl -n tma get pods -w
```

Expected steady state — note `2/2` on Aerospike, which is the server plus the
roster sidecar:

```
NAME                             READY   STATUS    RESTARTS   AGE
aerospike-0                      2/2     Running   0          90s
control-plane-6d4f8b7c9d-hq2vn   1/1     Running   0          85s
temporal-7c9d5f8b6-2xk4m         1/1     Running   0          85s
temporal-ui-5f7b8c9d4-pl9wq      1/1     Running   0          85s
```

Aerospike takes 30–60 s to reach `2/2`: its readiness probe refuses until the
strong-consistency namespace is in a staged roster (`ns_cluster_size=1`) with
no unavailable or dead partitions, which cannot happen until the roster sidecar
has run. (Measured on k3d: 35 s.)

**Then restart the control plane once.** All four pods report `Running` and the
demo is still not usable until you do:

```sh
kubectl -n tma rollout restart deployment/control-plane
kubectl -n tma rollout status  deployment/control-plane
```

`kubectl apply -f` starts the control plane and Temporal at the same instant, so
the control plane's one attempt to register the `demo` Temporal namespace lands
while Temporal is still binding its port. It logs

```
level=WARN msg="could not register the demo namespace at startup"
  error="... dial tcp 10.43.x.y:7233: connect: connection refused"
```

and does not try again, which leaves `GET /api/state` reporting
`"temporalReady": false` for good and the Run button returning
`503 demo worker is not running`. The restart is the whole fix — by then
Temporal is listening, the namespace registers, and the worker starts. This is
a control-plane bug (`control/server.go:78-86`, `Start` returns on the first
error instead of retrying), not a manifest one; until it is fixed the restart is
a required install step. Clicking **Switch** also clears it, because the switch
path re-runs the same register-and-start-worker sequence.

Verify before going further:

```sh
curl -s -u admin:change-me http://localhost:8080/api/state
# {"backend":"sqlite","temporalReady":true,"switching":false,"namespace":"demo"}
```

Set a real password before exposing the box to anything:

```sh
kubectl -n tma create secret generic control-plane-auth \
  --from-literal=username=admin \
  --from-literal=password="$(openssl rand -base64 24)" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n tma rollout restart deployment/control-plane
```

## 4. Open it

There is no `host` rule on the Ingress, so any name that reaches the box works:

```sh
kubectl -n tma get ingress demo
hostname -I | awk '{print $1}'     # on the box
```

- `http://<ip>/` — control plane
- `http://<ip>/temporal` — Temporal Web UI

On **k3d**, that is the port you published on the load balancer:
`http://localhost:8080/` and `http://localhost:8080/temporal`.

TLS is off. [`50-ingress.yaml`](50-ingress.yaml) has a commented cert-manager
block with the steps to turn it on once the box has a public DNS name.

## 5. Switch the backend

That is what the control plane's UI does, and it does it with this:

```sh
kubectl -n tma set env deployment/temporal TEMPORAL_ENVIRONMENT=aerospike
kubectl -n tma rollout status deployment/temporal
```

`sqlite` switches back. Both config files live in the same ConfigMap
([`20-temporal-config.yaml`](20-temporal-config.yaml)) and are mounted at
`/etc/temporal/config`; the binary passes `TEMPORAL_ENVIRONMENT` to Temporal's
config loader as `--env`, which loads `<value>.yaml`. Changing an env var
changes the pod template, so Kubernetes performs the restart — no ConfigMap
surgery, no `kubectl delete pod`.

The rollout strategy is `Recreate`, so the old server is gone before the new one
starts. Expect a few seconds of downtime per switch.

> **The two stores do not share data.** Workflows started on SQLite are not
> visible after switching to Aerospike, and the SQLite side is an `emptyDir`
> that is wiped on every restart. That is the demo, not a bug.

---

## Troubleshooting

### Which backend is live?

Three ways, cheapest first:

```sh
# what the Deployment is configured for
kubectl -n tma set env deployment/temporal --list | grep TEMPORAL_ENVIRONMENT

# what the running pod was actually started with -- differs from the above
# during a rollout, which is exactly when you care
kubectl -n tma get pods -l app.kubernetes.io/name=temporal \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].env[?(@.name=="TEMPORAL_ENVIRONMENT")].value}{"\t"}{.status.phase}{"\n"}{end}'

# what Temporal itself thinks -- the only answer that involves the store
kubectl -n tma run tctl --rm -it --restart=Never \
  --image=temporalio/admin-tools:1.31.2 -- \
  temporal operator cluster describe --address temporal:7233
```

The last one prints `PersistenceStore`, which reads `aerospike` or `sqlite`.

### Aerospike shows dead partitions

Symptom: `aerospike-0` sits at `1/2`, and Temporal crashloops with
`Node not found for partition temporal:4092` or times out on every read.

Confirm:

```sh
kubectl -n tma exec aerospike-0 -c aerospike -- \
  asinfo -v 'namespace/temporal' | tr ';' '\n' \
  | grep -E 'dead_partitions|unavailable_partitions|ns_cluster_size|strong-consistency'

kubectl -n tma exec aerospike-0 -c aerospike -- asinfo -v 'roster:namespace=temporal'
```

`ns_cluster_size=0` and `roster=null` mean the roster was never staged — the
partition counters read 0/0 in that state and are not evidence of health. That
is also why the readiness probe checks all three.

Dead partitions after a restart are **normal and expected**, and the sidecar
repairs them automatically — a strong-consistency namespace refuses to serve
data it cannot prove is complete, so any unclean stop (SIGKILL after the grace
period, node reboot, OOM kill) brings all 4096 back dead. If the sidecar ran,
they are already fixed by the time you look.

What `node-id` (pinned in the ConfigMap) prevents is the *other* failure: a
roster naming a node id that no longer exists, which is unrecoverable without
manual intervention and surfaces as `Node not found for partition
temporal:4092`. See `docs/04-open-questions.md` R16. If you see dead partitions
that the sidecar cannot clear, check that the ConfigMap still pins `node-id`.

Kick the sidecar:

```sh
kubectl -n tma logs aerospike-0 -c roster-init
kubectl -n tma delete pod aerospike-0        # sidecar re-runs on the new pod
```

To repair in place, by hand — this is what the sidecar does:

```sh
kubectl -n tma exec aerospike-0 -c roster-init -- bash -c '
  obs=$(asinfo -h 127.0.0.1 -v "roster:namespace=temporal" \
          | tr ":" "\n" | grep "^observed_nodes=" | cut -d= -f2)
  asinfo -h 127.0.0.1 -v "roster-set:namespace=temporal;nodes=$obs"
  asinfo -h 127.0.0.1 -v "revive:namespace=temporal"
  asinfo -h 127.0.0.1 -v "recluster:"
'
```

Use `asinfo`, never `asadm manage`: `manage` prompts for confirmation when the
namespace looks damaged and dies with `EOFError` without a TTY — which is
exactly when you need it. Note also that `roster:` separates fields with `:`
while `namespace/<ns>` uses `;`; parsing one with the other's separator
silently yields nothing.

`revive` is safe **only** on this single-node, RF1 setup: there is no diverged
replica to choose between. Do not automate it on a real cluster.

### `temporal-ui` CrashLoopBackOff: `cannot unmarshal !!str 'tcp://1...' into int`

Fixed in [`30-temporal-ui.yaml`](30-temporal-ui.yaml); here because the error
message points nowhere near the cause and the fix is easy to drop in a rewrite.

Kubernetes injects legacy Docker-link environment variables for every Service in
the namespace, as `<SERVICE>_PORT=tcp://<clusterIP>:<port>`. The Service is
called `temporal-ui`, so the pod is handed
`TEMPORAL_UI_PORT=tcp://10.43.x.y:8080` — and `TEMPORAL_UI_PORT` is also
ui-server's own config key for the port it listens on. It reads the injected
value, fails to parse `tcp://…` as an int, and exits before printing anything
that mentions Kubernetes:

```
config file corrupted: yaml: unmarshal errors:
  line 6: cannot unmarshal !!str `tcp://1...` into int
```

`enableServiceLinks: false` on the pod spec removes the collision class; the
manifest also sets `TEMPORAL_UI_PORT: "8080"` explicitly, since an explicit env
var beats an injected link. Confirm the injection with:

```sh
kubectl -n tma exec deployment/control-plane -- env | grep _PORT=
```

### `ErrImageNeverPull` / `ImagePullBackOff` on temporal or control-plane

The image was not imported into containerd, or was imported into dockerd only.
`docker images` is not what k3s looks at:

```sh
sudo k3s ctr images ls | grep temporal-meets-aerospike
```

If it is missing, redo step 2. If it is present and pods still fail with
`exec format error`, the image is the wrong architecture — cross-build it.

### Temporal crashloops right after a switch to `aerospike`

Check whether Aerospike is actually serving before blaming the store:

```sh
kubectl -n tma get pods -l app.kubernetes.io/name=aerospike   # want 2/2
kubectl -n tma get endpoints aerospike                        # want an IP, not <none>
kubectl -n tma logs deployment/temporal --tail=50
```

The `aerospike` Service is headless and only lists ready pods, so
`endpoints … <none>` means the readiness probe is failing — go to the dead
partitions section above.

### PVC stuck `Pending`

`local-path` provisions on first pod schedule, so a `Pending` PVC with no pod
is normal. A `Pending` PVC *with* a pending pod usually means disk:

```sh
kubectl -n tma describe pvc data-aerospike-0
df -h /var/lib/rancher/k3s/storage
```

The namespace preallocates its storage file at `filesize` (2G in
[`10-aerospike-config.yaml`](10-aerospike-config.yaml)); the PVC asks for 4Gi.

### Running the conformance suites against this cluster

The suites run on your machine and connect over a port-forward, so they need
the node's `alternate-access-address` rather than its pod IP:

```sh
kubectl -n tma port-forward aerospike-0 3000:3000
# then, with the suite configured for useServicesAlternate: true
go test ./conformance/...
```

### Starting over

```sh
kubectl delete namespace tma      # takes the PVC with it
k3d cluster delete tma            # k3d: the whole cluster, images and all
```


## Deploying to a Civo VPS (what was actually done)

A Civo Compute instance with k3s installed by hand, not Civo's managed
Kubernetes. The reason is image delivery: Civo
[does not support SSH to managed Kubernetes nodes](https://www.civo.com/docs/faq),
which removes `k3s ctr images import` and building on the box, leaving only
"push to a registry" for two images this project never pushes anywhere. A plain
instance keeps this README working as written and is cheaper than a managed node
plus the load balancer its ingress needs.

Target used: `g4s.large` — 4 vCPU, 8 GB, Ubuntu 24.04, amd64. **8 GB, not 4.**
The ceilings sum to roughly 5.5 GB (Aerospike `filesize 2G`, Temporal's 1536Mi
limit, and k3s wanting 2 GB for a server node), so a 4 GB box runs out of
headroom exactly when someone is watching.

```sh
# 1. k3s. Brings Traefik and local-path with it, so ingress and the PVC need
#    nothing further.
curl -sfL https://get.k3s.io | sudo sh -s - --write-kubeconfig-mode 644

# 2. A builder. Build on the box: it is amd64, and cross-building from an
#    arm64 laptop under emulation is far slower.
sudo apt-get update && sudo apt-get install -y docker.io
sudo usermod -aG docker "$USER"

# 3. Ship the source and build both images natively.
rsync -az --exclude .git ./ user@BOX:~/tma/
ssh user@BOX 'cd ~/tma
  sudo docker build -f deploy/Dockerfile               -t temporal-meets-aerospike/server:dev        .
  sudo docker build -f deploy/Dockerfile.control-plane -t temporal-meets-aerospike/control-plane:dev .
  sudo docker save temporal-meets-aerospike/server:dev        | sudo k3s ctr images import -
  sudo docker save temporal-meets-aerospike/control-plane:dev | sudo k3s ctr images import -'

# 4. Apply, then create both credentials ON the box so they never reach git.
sudo k3s kubectl apply -f deploy/k8s/
PASS=$(openssl rand -base64 18)
sudo k3s kubectl -n tma create secret generic control-plane-auth \
  --from-literal=username=demo --from-literal=password="$PASS" \
  --dry-run=client -o yaml | sudo k3s kubectl apply -f -
sudo k3s kubectl -n tma create secret generic ingress-basic-auth \
  --from-literal=users="demo:$(openssl passwd -apr1 "$PASS")" \
  --dry-run=client -o yaml | sudo k3s kubectl apply -f -
```

All four pods were ready 30 seconds after apply.

**Two credentials, deliberately.** `control-plane-auth` is the Go service's own
check; `ingress-basic-auth` backs the Traefik middleware in
[`45-ingress-auth.yaml`](45-ingress-auth.yaml). The second is not redundant: the
ingress routes `/temporal` straight to the Temporal Web UI, which never runs the
Go service's auth code. Without the middleware that UI is world-readable and
world-terminatable. Verify with an unauthenticated request — `/`, `/temporal`
and `/api/state` must all return 401.

**Civo's firewall** must allow 80 and 443 inbound; SSH alone is the default on
some profiles.

### TLS

Until a certificate is installed the basic-auth password crosses the wire in
plaintext on every request. Do not circulate the URL before TLS is on.

`<IP>.sslip.io` resolves to the address with no DNS setup and satisfies an
ACME HTTP-01 challenge, which is enough for a throwaway box. For a real
hostname, point an A record at the instance and follow the cert-manager notes at
the bottom of [`50-ingress.yaml`](50-ingress.yaml).
