# hive-compose

docker compose **sections** as a hive IAddon. Name what you want to work on, a
service or a native compose profile, and exactly that runs, closed over the
compose file's `depends_on`. Take it down later and only that section goes;
anything another running section needs stays up. An idle reaper stops sections
nobody touched within their TTL, so no stack keeps loading the machine.

Your compose YAML is the source of truth. The addon never parses it: compose
itself resolves every closure (`config --format json <svc>`), lists services and
profiles (`config --services`, `config --profiles`) and reports what runs
(`ps --format json`).

## Setup: point it at your compose projects

`config.edn` → `:addons {"hive.compose" {...}}`:

```clojure
{:compose/projects
 [{:project/id "sisf"
   :project/dir "~/PP/funeraria/dc"
   :project/files ["docker-compose.yml" "docker-compose.override.yml"]  ; optional
   :project/ttl-minutes 45}]}                                            ; optional
```

## Use: name a target

```
compose targets                      # always-on services + native profiles per project
compose up target=sisf-web           # section sisf/sisf-web: sisf-web + its depends_on closure
compose up target=crm                # a native profile: its own services + closure,
                                     #   NOT the always-on base a bare --profile adds
compose up target=sisf/sisf-web,sisf-crm   # several targets, one section
compose switch target=sisf-crm       # stop what only the current section needed
compose down target=sisf-web         # remove that section's containers (volumes never)
compose status                       # active sections, idle time, reaper countdown
```

## Declare runtime needs in the YAML

A section is exactly compose's closure, so what an app calls at runtime belongs in
its `depends_on`. For a soft dependency (start it with the app, but don't fail
without it) use compose's `required: false`:

```yaml
sisf-web:
  profiles: [frontend]
  depends_on:
    envoy:
      condition: service_started
      required: false
```

With that, `up target=sisf-web` brings envoy and envoy's own closure (auth,
keycloak, postgres, redis...) and nothing else.

## Tool `compose`

| command | effect |
|---|---|
| `targets` | what each configured project offers |
| `up target=… \| profile=…` | start a section alongside whatever runs |
| `switch target=…` | make it current; stop what only the previous section needed |
| `down` / `stop` | remove / stop a section's containers, sparing services other active sections need |
| `touch` | reset a section's idle clock (`ps` and `logs` touch too) |
| `ps` / `logs tail=N` | inspect |
| `status` | active sections, idle time, seconds until reaped |
| `reap` | run the reaper now |
| `adopt` | take charge of running containers nobody owns |
| `projects` | every compose project on the host, managed ones marked |
| `reload` | re-read config |

Hooks for other addons: `:compose/touch!`, `:compose/status`, `:compose/tick!`.

## Presets (optional)

Named presets in `~/.config/hive-mcp/compose-profiles.edn`, used with `profile=ID`:

```clojure
[{:profile/id "shop-api"
  :profile/dir "~/code/shop"
  :profile/services ["api"]
  :profile/ttl-minutes 30
  :profile/idle-action :stop}]      ; :stop (default) | :down | :none
```

## Safety

- Only sections the addon started or adopted are ever reaped. Adoption covers
  presets that are running and the running containers of configured projects that
  no section accounts for (section `<project>/adopted`); unconfigured projects are
  listed, never touched.
- `down` never passes `-v`: volumes survive every operation.
- An empty service set never reaches docker (a bare `compose stop` would stop the
  whole project).
- The active map lives in `~/.local/state/hive-compose/state.edn`, so a restart
  still reaps what it started.

## Config

`:compose/projects`, `:compose/default-ttl-minutes` (60),
`:compose/default-idle-action` (`:stop`), `:compose/tick-seconds` (60),
`:compose/profiles`, `:compose/profiles-file`, `:compose/state-file`,
`:compose/timeout-ms` (60000), `:compose/up-timeout-ms` (600000),
`:compose/adopt?` (true).

## Development

```sh
clojure -M:test          # unit suite (stub engine)
clojure -M:integration   # against the local docker daemon
```

MIT. Depends on hive-addon, hive-dsl and hive-system (MIT).
