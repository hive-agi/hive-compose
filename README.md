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

## Host programs: what runs beside the containers

A dev stack is rarely containers alone: a shadow-cljs watch or a JVM runs on the
host and the containers proxy to it. Declare those as programs of the project,
and a section starts them once its containers are up:

```clojure
{:compose/projects
 [{:project/id "sisf"
   :project/dir "~/PP/funeraria/dc"
   :project/programs
   [{:program/id "sisf-web"
     :program/dir "../sisf-web"          ; relative to :project/dir, or absolute, or ~
     :program/with ["envoy"]}            ; starts with a section that runs envoy
    {:program/id "inventory"
     :program/dir "../inventory"
     :program/kind :deps                 ; the directory also has a shadow-cljs.edn
     :program/with ["inventory-frontend-dev"]}]}]}
```

A directory with a Clojure build file needs no command. The build file decides
how it starts, and it always starts with an nREPL:

| build file | starts as | nREPL port |
|---|---|---|
| `shadow-cljs.edn` | `npx shadow-cljs watch <every build>` | its `:nrepl {:port N}` |
| `deps.edn` with an `:nrepl` alias | `clojure -M:nrepl` | the alias's `--port` |
| `deps.edn` without one | `clojure -Sdeps <nrepl + cider-nrepl> -M -m nrepl.cmdline` | `:program/nrepl-port`, else the `.nrepl-port` it writes |
| `project.clj` | `lein repl :headless` | `:program/nrepl-port` |
| `bb.edn` | `bb nrepl-server` | `:program/nrepl-port` (1667) |

`:program/command` (an argv or a shell string) replaces the detected command,
and the port is still read from the build file; any other directory needs one.
`:program/kind` picks the build file when a directory has several,
`:program/env` adds environment, `:program/with` names the services the program
pairs with (none: every section of the project).

`up` answers each program's pid, log and nREPL port; `status` and `programs`
say whether the process lives and whether the port answers yet. A program two
sections want runs once and stops with the last of them, on `down`, `stop`,
`switch` or the reaper. It runs detached in its own process group, with output
appended to `~/.local/state/hive-compose/logs/<project>-<program>.log`, and is
tracked by pid, so it survives a restart of the host and is still stopped
afterwards. A program that fails to start is reported and never fails the
section.

## Tool `compose`

| command | effect |
|---|---|
| `targets` | what each configured project offers |
| `programs` | the host programs each project declares, how each starts, and whether its nREPL answers |
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
`:compose/adopt?` (true), `:compose/programs?` (true), `:compose/log-dir`,
`:compose/stop-grace-ms` (5000, before a program's group is killed).

## Development

```sh
clojure -M:test          # unit suite (stub engine)
clojure -M:integration   # against the local docker daemon
```

MIT. Depends on hive-addon, hive-dsl and hive-system (MIT).
