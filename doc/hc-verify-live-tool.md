# Live-tool scenario run (hc-verify2, 2026-11-22)

Run through the LIVE `compose` MCP tool in the hive JVM (hive.compose at
staging 2b756c2, remounted to c2da218 during S12), on the real docker daemon.
Fixtures were declared temporarily in `~/.config/hive-mcp/config.edn`
(`:addons "hive.compose"`): provider `hcv-kafka` (compose project `hcvlkp`,
apache/kafka:3.9.1 KRaft, -Xmx512m, mem_limit 900m) and consumers `hcv-a`,
`hcv-b`, `hcv-c` (`:share? false`), stacks `hcv-ab`, `hcv-a`, `hcv-b`,
`hcv-c`. The config was restored byte-for-byte from its backup afterwards.

## S1 (live): PASS
- `plan stack=hcv-ab run=s1live01`: 1 instance, 1 shared, 0 private, to-start +kafka.
- `stack-up`: one kafka container (`hcvlkp-kafka-1`); `kafka` resolves from A
  to 172.26.0.2 on hive-s1live01-hcv-a and from B to 172.27.0.2 on
  hive-s1live01-hcv-b (the kafka's IP on each view network); each consumer is
  on its own view network only.
- KAFKA_TOPIC_PREFIX s1live01.hcv-a. / s1live01.hcv-b., KAFKA_BROKERS kafka:9092,
  HIVE_RUN_ID s1live01. A produced `hello-live-a` to s1live01.hcv-a.orders and
  B consumed it from the same broker.

## S12 (live hot reload): PASS
- With run s1live01 up, `hot reload addon=hive.compose` (strategy remount,
  data-preserved? true, reloaded hive-compose.addon plus the new hc10 why
  namespaces).
- After the remount, `stack-status run=s1live01` still shows status up,
  kafka healthy and both consumers running; container ids and StartedAt of all
  three containers are unchanged (nothing was recreated).
- Refcount state survived the remount: a run started after the reload
  (s12post01, stack hcv-a) shared the same kafka, and `stack-down s1live01`
  answered kept {kafka [[s12post01 hcv-kafka/kafka]]}, removed only the
  s1live01 view networks, and left the kafka running with the same id.

## S4 (live refcount release): PASS
- Runs s12post01 (a), s4bl01 (b), s4cl01 (c, private) at once. c got
  `hcvlkp-hcv-c-s4cl01-kafka-1`, a different container id; from C `kafka`
  resolves to the private kafka's IP; the shared kafka is not on C's network.
- down a: kept for s4bl01, same container id, still running.
- down b: released hcv-kafka/kafka; the shared container is gone.
- down c: released the private project hcvlkp-hcv-c-s4cl01.
- `stack-status` empty afterwards. docker ps -a and network ls match the
  pre-run snapshot. No volume was removed.

## Findings (minor)
- Each apache/kafka container leaves 3 anonymous volumes (the image declares
  VOLUME /etc/kafka/secrets, /mnt/shared/config, /var/lib/kafka/data); 21 were
  added over 7 containers. Correct under the never-remove-volumes rule, but
  stack-down could say so, or fixtures could mount named volumes.
- After the fixtures were removed from config.edn, `compose reload` dropped
  the projects but kept the old `:compose/stacks` (the diff reported nothing),
  so `plan stack=hcv-ab` answers unknown-consumer instead of unknown stack.
  Likely the remounted addon's runtime config still carries the block it was
  declared with at init. Harmless; worth a card.
- `compose reload` reads the host's cached config: after editing config.edn,
  run `config reload` first, otherwise the reload reports "nothing changed".
