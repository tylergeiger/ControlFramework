# Local Test Mode

A self-contained, single-host deployment of the FABRIC Control Framework
orchestrator stack for local development and testing.

It accepts any well-formed bearer token without verifying its signature. Other
request handling — parameter validation, project-id handling, slice lifecycle,
resource checks — runs normally. Token claims (`sub`, `email`, `projects`, …) are
read from the token you supply.

## What "test mode" changes

The bypass is driven by configuration:

| Setting (in `testmode/config/*.yaml`) | Effect |
| ------------------------------------- | ------ |
| `oauth.verify-sig: False`             | Skip JWT signature check + token-revoke-list lookup (which needs an external Credential Manager). Claims are decoded from the token as-is. |
| `oauth.verify-exp: False`             | Don't reject expired tokens. |
| `pdp.enable: False`                   | Skip the external Policy Decision Point authorization call. |
| `kafka-security-protocol: PLAINTEXT`  | Talk to the in-compose Kafka broker without SSL certificates. |
| AM `handler: MockAMHandler`           | Emulate provisioning and return configurable fake management IPs instead of driving real substrate (see [Aggregate Managers and the mock substrate](#aggregate-managers-and-the-mock-substrate)). |

`verify-sig` defaults to `True` when unset.

## Bring up the stack

```bash
docker compose -f docker-compose-testmode.yaml up --build
```

Services:

| Component      | URL / port |
| -------------- | ---------- |
| Orchestrator REST API | http://localhost:8700 |
| Kafka (host listener) | localhost:19092 |
| Schema Registry       | http://localhost:8081 |
| Postgres              | localhost:5432 (user `fabric` / pass `fabric`) |
| Neo4j (orchestrator)  | http://localhost:7474 (neo4j/password) |

Tear down (including volumes):

```bash
docker compose -f docker-compose-testmode.yaml down -v
```

## Aggregate Managers and the mock substrate

The `site1-am` and `net1-am` services are FABRIC `Authority` actors — they run the
orchestrator/AM message exchange (redeem, extend, modify, close, poa) and read
their Avro schemas from the Schema Registry. Only the substrate boundary is
mocked: instead of the ansible-backed handlers that drive hardware, they use
`MockAMHandler`
([fabric_cf/actor/handlers/mock_am_handler.py](../fabric_cf/actor/handlers/mock_am_handler.py)),
which returns a fake, configurable management IP for each compute sliver. The
reservation goes `Active` and the owning slice reaches `StableOK`.

The fake IPs are configured the same way as any other handler, via the
`properties` block of the resource `handler` in the AM config:

```yaml
handler:
  module: fabric_cf.actor.handlers.mock_am_handler
  class: MockAMHandler
  properties:
    management-ip-pool: 10.20.0.0/16   # per-node IP from this pool (default)
    # management-ip: 192.0.2.50        # or pin every node to one fixed address
```

Each AM also needs an **ARM advertisement model** (`*.graphml`) describing the
resources it manages. Representative models are shipped under `./neo4j` and are
mounted by default:

* `./neo4j/RENCI-ad.graphml`  (site AM, site `RENC`) — override with `SITE_ARM`
* `./neo4j/Network-ad.graphml` (net AM)              — override with `NET_ARM`

Point the env vars at your own models to advertise different resources:

```bash
SITE_ARM=/path/to/site-ad.graphml NET_ARM=/path/to/net-ad.graphml \
  docker compose -f docker-compose-testmode.yaml up --build
```

### The provisioning loop is closed automatically

FABRIC advertises resources with a **pull** model: on startup each AM loads its
ARM and creates a source delegation, but advertises it *locally* with no callback
(`Cannot generate update: no callback` in the AM log — this is expected). The
broker only learns about those resources once it explicitly **claims** each
delegation via the `claim_delegations(broker, did)` management operation, which
merges it into the broker's Combined Broker Model (CBM).

The one-shot **`claim`** service performs this step for you: it waits for both
AMs to advertise, claims their delegations at the broker over Kafka — using the
same `KafkaBroker`/`KafkaActor` management proxies the FABRIC CLI uses — and
exits. So `docker compose -f docker-compose-testmode.yaml up` yields a broker CBM
populated with the AMs' resources and `GET /resources` returns the advertised
model, with no manual step. The claim is idempotent; re-run it any time with:

```bash
docker compose -f docker-compose-testmode.yaml up claim
```

## Rebuilding after a code change

The actor `Dockerfile`s declare `VOLUME ["/usr/src/app"]`, and `python -m` imports
the code from that path. Because the anonymous volume backing `/usr/src/app` is
populated from the image only when first created, a plain `up --build` after a
code change can keep running the **old** code from a stale anonymous volume.
After changing Python sources, recreate with renewed anonymous volumes:

```bash
docker compose -f docker-compose-testmode.yaml up -d --build --force-recreate --renew-anon-volumes <service>
# or, for a clean slate (also wipes the databases):
docker compose -f docker-compose-testmode.yaml down -v
docker compose -f docker-compose-testmode.yaml up -d --build
```

A first-time `up --build` on a clean machine is unaffected.

## Minting a test token

Any token the orchestrator can decode is accepted. The signature is ignored, so
sign with whatever you like. The payload must carry the claims the orchestrator
reads:

```python
import jwt, time

payload = {
    "sub": "test-user-sub",
    "email": "tester@example.com",
    "uuid": "00000000-0000-0000-0000-000000000001",
    "projects": [
        {
            "uuid": "11111111-1111-1111-1111-111111111111",
            "name": "TestProject",
            "tags": ["Slice.Create", "Slice.Modify", "Slice.Delete"],
        }
    ],
    "iat": int(time.time()),
    "exp": int(time.time()) + 3600,
}

# The signature is not verified; the key is irrelevant.
token = jwt.encode(payload, "irrelevant-secret", algorithm="HS256")
print(token)
```

Use it against the API:

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8700/version
curl -H "Authorization: Bearer $TOKEN" http://localhost:8700/slices
```

Change the `projects`/`tags`/`email` in the payload and the orchestrator honors
those claims.
