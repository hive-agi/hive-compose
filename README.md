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
section. A program whose nREPL port already answers is somebody else's: it is
reported as `external?`, not started a second time and never stopped.

## Environment and secrets

`:project/env`, `:profile/env` (presets), `:program/env` and the `env` param of
`up`/`switch`/`restart` take `{VAR value}`. A value is a string, or a secret
reference read from [pass](https://www.passwordstore.org/):

```clojure
{:project/id "dc"
 :project/dir "~/PP/funeraria/dc"
 :project/env {"CRM_WA_ACCESS_TOKEN" {:pass "Funeraria/Whatsapp/token"}
               "SISF_CRM_SRC" "../../funeraria-wt/dc-staging-view-sisf-crm"}}
```

```
compose up target=sisf-crm env={"EXTRA_TOKEN": {"pass": "Funeraria/other"}}
```

A reference is read (`pass <path>`, first line, trimmed) only when containers
are created (`up`, `switch`, `restart recreate=true`) or a host program starts,
and the value goes to that process alone. Every other compose call (`config`,
`ps`, `stop`, `down`, `logs`, an in-place `restart`) is given the rendering
`pass:<path>`, so interpolation still works and no store is read. The value is
never kept in the state file, never shown by `status`, `ps`, `targets`,
`programs` or `logs`, and what docker prints has it replaced by the rendering.
An entry that cannot be read (missing, gpg locked, `pass` absent) fails that
`up` before docker runs, naming the variable, the reference and the reason; a
program whose env cannot be read is reported and not started.
`:compose/resolver` injects the stores (`{:pass ISecretBackend}`) in place of
`pass`.

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
| `claim target=… note=…` | an agent declares it works on a section (bringing it up if needed); answers the shared nREPL per module |
| `release target=… [stop=true]` | give the claim up; with none left the section is orphaned, or stopped now with `stop` |
| `who` | per section: claims, what is using it, when it is reaped; per agent: the sections it works in |
| `nrepl path=…` | which shared nREPL drives that file or directory (no path: every route) |
| `test suites=… projects=…` | run the suite x project matrix; the answer starts with `plan {projects suites cells services}`. Neither `suites` nor `projects`: refused unless `all=true` (`dry_run=true` plans it anyway) |

**Strict params.** Every command refuses a param it does not take, before
anything runs, over MCP and the HTTP API alike: `invalid-param` with
`unknown`, `valid` (the command's params, from `command-params`) and
`did-you-mean` (`suite` -> `suites`, `project` -> `projects`). Only the keys
the MCP host adds itself are always accepted: `agent_id`, `directory`,
`async`, `async-timeout-ms`, `_meta` and any `_`-prefixed key
(`hive-compose.api/host-params`). A `test` matrix that ran no cell (an error
before the first one) takes down the stack it started, unless `keep_up`,
another agent's claim, or a reused `run=`.

Hooks for other addons: `:compose/touch!`, `:compose/status`, `:compose/tick!`,
`:compose/claim!`, `:compose/release!`, `:compose/nrepl-for`, `:compose/who`.

## Swarm work

Several agents working on one app should share its running JVMs, not each boot
their own, and an app nobody works on should not keep holding memory.

- **Claims.** `compose claim target=shop/api note="fix login"` brings the section up
  if needed and records the claim (the agent is the call's `agent`, else its
  `agent_id`). The answer lists the section's nREPL **routes**: for each program
  directory, the port to drive it through (`clojure_eval port=…`) and one line
  of advice. A claim must be renewed (claim again) within
  `:compose/claim-ttl-minutes` (30); a claim made by a swarm agent also ends
  when that agent leaves the swarm.
- **Routes.** `compose nrepl path=/w/shop/api/src/core.clj` answers the program
  whose directory holds the file most closely, whether its nREPL answers, and
  whether somebody outside the addon runs it (`:external?`, shared all the same).
- **Usage.** Every reaper pass counts as use of a section: claims that hold, swarm
  agents whose cwd lies in the project's directories, and connections newly opened
  to its nREPL ports (`/proc/net/tcp`; a connection left open counts once).
  `:compose/activity #{:nrepl :published}` also counts new connections to the
  ports its containers publish (opt-in: an app's own connection pool would
  otherwise look like use). What is in use has its idle clock reset; `who`
  shows why.
- **Notices.** When an agent starts working in a project whose section serves a
  ready nREPL, the swarm is told once which port drives which module. `up` and
  `switch` announce the routes, and the reaper announces what it freed.
- **Enforcement.** Nothing in use for the TTL is stopped, as before. A section whose
  last claim ended (released, stale or its agent gone) is **orphaned** and reaped
  after `:compose/orphan-ttl-minutes` (10) of disuse instead. `release stop=true`
  frees it at once, sparing what other sections need.

With no hive host (published standalone), the swarm adapter answers absent and the
addon runs on claims and connections alone.

## Presets (optional)

Named presets in `~/.config/hive-mcp/compose-profiles.edn`, used with `profile=ID`:

```clojure
[{:profile/id "shop-api"
  :profile/dir "~/code/shop"
  :profile/services ["api"]
  :profile/ttl-minutes 30
  :profile/idle-action :stop}]      ; :stop (default) | :down | :none
```

A preset carries its own `:profile/files` and `:profile/env` (`{:pass ..}`
values included), so an overlay compose file and the secrets it needs apply
only when that preset runs, never to the project's targets. To run the
overlay against the same containers, give it the project's compose name:

```clojure
{:profile/id "crm-whatsapp"
 :profile/dir "~/PP/funeraria/dc"
 :profile/project "dc"
 :profile/files ["docker-compose.yml" "docker-compose.override.yml"
                 "docker-compose.crm-whatsapp.yml"]
 :profile/services ["sisf-crm"]
 :profile/env {"CRM_WA_ACCESS_TOKEN" {:pass "Funeraria/Whatsapp/token"}}}
```

`compose up profile=crm-whatsapp` then runs
`docker compose -p dc -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.crm-whatsapp.yml up -d sisf-crm`
with the token read from pass at that moment. `compose down profile=crm-whatsapp`
releases it.

`:profile/project` only names the compose project. A preset does not inherit that
project's `:project/env` from the addon config, so repeat in `:profile/env` every
value the preset's services need.

Precedence, weakest to strongest: `:project/env` (target sections only) <
`:profile/env` < the `env` of a call. The env a call gave (`up`, `switch`, a
recreating `restart`) is remembered by the section (`:profile/call-env` in the
state file, `pass` references kept as references), so a later `up` (by target
or profile), `restart` or reaper pass of the same section never falls back to
weaker env; `status` lists those vars as `call-env-keys`. `env={"VAR": null}`
forgets VAR (a no-op when it is not remembered), and a `reload` that changes
VAR's declared value (preset or project env) forgets it too, reporting
`call-env-dropped {section [VAR]}`. An in-place `restart` recreates nothing, so
its env is not remembered. `restart target=<svc>` (and `up`/`switch target=`
of a section not yet active) of a service an active section holds, orphaned or
not, runs with that section's dir, files and env, and a recreate's env is
remembered by that section; when several hold it with different inputs the
call is refused (`holders-disagree`, naming them) and `profile=<id>` picks one.
`status` shows an active section's `dir`, `files`, `env-keys` (names only) and
`env-src` (the value of each `*_SRC` variable that looks like a path, the
checkout under test). An `allow_shared` answer says per shared service whether
the engine `recreated`, `kept`, `created` it or left it `absent`.

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
- `up`, `switch` and `restart` refuse while orphans of a dead rootless docker
  daemon still hold the section's docker volumes or host ports. An orphan is a
  container process (subuid range, or a `docker-<id>.scope` cgroup) whose
  parent is `systemd --user` and that no running container owns. The refusal
  names each orphan with its pids, because recreating the service would run a
  second copy on the same volume, e.g. two postmasters on one data dir.
  `allow_orphans=true` goes ahead anyway and reports what it overrode. The
  check reads `/proc` and `/etc/subuid`; when it cannot read them it says so
  and lets the call through. `:compose/orphan-guard? false` turns it off.
- A section never takes a service from under another live section of the same
  compose project. compose keeps one container per service, so `restart
  recreate=true` of a service another section holds (it runs it or needs it),
  or an `up`/`switch` whose compose inputs (project dir, files, env, references
  compared by rendering) differ from those the holder ran with, which makes
  compose recreate the container on a changed config hash, is refused. The
  refusal names the holder section, the agents owning it (its claims, else the
  agents the last usage pass saw working in it), the services and which inputs
  differ (variable names, never values). A holder is live unless it is
  orphaned and unused. Sections with equal inputs share dependencies freely.
  `allow_shared=true` goes ahead and reports what it overrode. The decision is
  the pure `hive-compose.promote.ownership/conflicts`.

## Resource caps (stacks)

A stack can be capped so one run never takes the host down. Every key is
optional, and a config without them runs uncapped as before.

```edn
{:compose/default-resources {:mem "512m"}      ; any instance nobody caps
 :compose/budget "12g"                         ; all of a stack's capped memory
 :compose/host-reserve "4g"                    ; MemAvailable kept free
 :compose/projects
 {"infra" {:project/provides {"kafka" {:service "kafka" :resources {:mem "1g" :cpus 2}}
                              "search" {:service "es" :resources :unbounded}}}
  "shop"  {:project/needs {"kafka" {:resources {:mem "2g" :jvm? true}}}}}
 :compose/stacks
 [{:stack/id "shop" :stack/consumers ["shop"]
   :stack/resources {"kafka" {:mem "3g"}} :stack/budget "6g" :stack/priority 1}]}
```

- Precedence per key: `:stack/resources`, then the Need, then the Provide, then
  `:compose/default-resources`. `:unbounded` opts out: no cap is written and
  the instance is not summed.
- A shared instance takes the largest cap of its consumers. `compose plan`
  prints each instance's cap, the total, and what sharing saves against one
  private copy per consumer.
- `stack-up` refuses with `:compose/over-budget` (`:basis` `:stack`, `:budget`
  or `:host`, plus `:limit`, `:total-mem`, `:over` and `:instances`) before
  anything starts. The host limit is MemAvailable minus `:compose/host-reserve`,
  compared with what is not yet running.
- A private instance gets `mem_limit` and `cpus` in its stack override. JVM
  services (`:jvm? true`, or a detected java image) also get
  `JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75`, merged per flag, and the
  service's own flags win. A shared instance runs from its own files, so its
  cap is summed and admitted but not written.
- Reaper pressure guard: while MemAvailable is below `:compose/host-reserve`,
  each pass stops the idle, unclaimed run whose stack has the lowest
  `:stack/priority`.

## Config

`:compose/projects`, `:compose/default-ttl-minutes` (60),
`:compose/default-idle-action` (`:stop`), `:compose/tick-seconds` (60),
`:compose/profiles`, `:compose/profiles-file`, `:compose/state-file`,
`:compose/timeout-ms` (60000), `:compose/up-timeout-ms` (600000),
`:compose/adopt?` (true), `:compose/programs?` (true), `:compose/log-dir`,
`:compose/stop-grace-ms` (5000, before a program's group is killed),
`:compose/program-grace-ms` (3000, how long a started program is watched; one
that exits within it is reported `:status :exited` with `:exit-code`,
`:log-tail` and `:advice`, and is not held; `status` reports a held program
that died since the same way),
`:compose/swarm?` (true), `:compose/notify?` (true), `:compose/activity`
(`#{:nrepl}`), `:compose/cpu-busy-percent` (5), `:compose/claim-ttl-minutes` (30),
`:compose/orphan-ttl-minutes` (10).

`:compose/activity` says which new connections count as use; a project
overrides it for its sections with `:project/activity` (a preset with
`:profile/activity`). `#{:nrepl :published}` also counts new peers on the host
ports the section's containers publish, so a browser or any host-side client
keeps it alive. Peers whose address belongs to one of the project's own
running containers (`docker inspect`) are left out, so pools do not count, and
a keep-alive connection counts once, when first seen. `who` and `status` name
the source that counted (`:in-use-via`, e.g. `[:published]`) and each
section's `:activity`.

`:cpu` in the activity counts a section's running services busier than
`:compose/cpu-busy-percent` (5, percent of one core) as use, whether or not a
client is connected: a batch job or a queue worker that nobody touches is not
reaped while it works. Each reaper pass takes one `docker stats --no-stream`
sample of the project's running containers (found by compose label) and keeps
the busiest container per service; `who` shows the reason as
`{:service "worker" :cpu 80.0 :source :cpu}`. An engine that cannot read the
load counts nothing, as before.

## Development

```sh
clojure -M:test          # unit suite (stub engine)
clojure -M:integration   # against the local docker daemon
```

MIT. Depends on hive-addon, hive-dsl and hive-system (MIT).
