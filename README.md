# hive-compose

docker compose profiles as a hive IAddon. A **profile** is a named set of services to
run and test an app locally: one compose project, its compose files, native compose
`profiles:`, and optionally a subset of services. The addon brings a profile up,
switches between profiles in one call, and runs an **idle reaper** so no stack keeps
loading the machine in the background.

It is a thin policy layer over `docker compose`. Compose resolves which services a
profile needs (`config --format json`, dependency closure included) and reports what
runs (`ps --format json`); YAML is never parsed here.

## Profiles

`~/.config/hive-mcp/compose-profiles.edn` (or inline under `:compose/profiles`):

```clojure
[{:profile/id "shop-api"
  :profile/dir "~/code/shop"                 ; compose project directory
  :profile/files ["compose.yml" "compose.dev.yml"]   ; optional, default compose lookup
  :profile/services ["api"]                  ; optional subset; deps come along
  :profile/ttl-minutes 30                    ; optional, default 60
  :profile/idle-action :stop                 ; :stop (default) | :down | :none
  :profile/description "API + postgres + keycloak"}

 {:profile/id "shop-full"
  :profile/dir "~/code/shop"
  :profile/compose-profiles ["frontend" "debug"]   ; native compose profiles
  :profile/env {"TAG" "dev"}
  :profile/wait? true}]                      ; `up --wait` for healthchecks
```

## Tool `compose`

| command | effect |
|---|---|
| `status` | every profile: active?, services, idle time, seconds until reaped |
| `up profile=X` | start X alongside whatever runs |
| `switch profile=X` | make X current; stop what only the previous profile needed |
| `down profile=X` / `stop profile=X` | remove / stop X's containers, sparing services other active profiles need |
| `touch profile=X` | reset X's idle clock (`ps` and `logs` touch too) |
| `ps profile=X` / `logs profile=X tail=N` | inspect |
| `reap` | run the reaper now |
| `adopt` | take charge of running stacks of configured profiles |
| `projects` | every compose project on the host, managed ones marked |
| `reload` | re-read the profiles file |

Hooks for other addons: `:compose/touch!`, `:compose/status`, `:compose/tick!`.

## Safety

- Only profiles this addon started or adopted are ever reaped; other compose projects
  are listed, never touched.
- `down` never passes `-v`: volumes survive every operation.
- An empty service set never reaches docker (a bare `compose stop` would stop the
  whole project).
- The active map lives in `~/.local/state/hive-compose/state.edn`, so a restart still
  reaps what it started; stacks started by hand for a configured profile are adopted
  on the first reaper pass.

## Config (`:addons {"hive.compose" {...}}`)

`:compose/default-ttl-minutes` (60), `:compose/default-idle-action` (`:stop`),
`:compose/tick-seconds` (60), `:compose/profiles-file`, `:compose/state-file`,
`:compose/timeout-ms` (60000), `:compose/up-timeout-ms` (600000), `:compose/adopt?` (true).

## Development

```sh
clojure -M:test          # unit suite (stub engine)
clojure -M:integration   # against the local docker daemon
```

MIT. Depends on hive-addon, hive-dsl and hive-system (MIT).
