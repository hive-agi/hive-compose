#!/usr/bin/env python3
"""Assemble resources/hive_compose/openapi.yaml (hc-14; served at GET /v1/openapi.yaml) from:

  GENERATED  staging.components.generated.json  malli.json-schema of hive-compose.schema @ origin/staging
  GENERATED  staging.tool.generated.json        the MCP tool's live :inputSchema + command-help
  HAND       RESPONSES / ERRORS / PLANNED below, read off pipeline/ops.clj, pipeline/swarm.clj,
             pipeline/restart.clj, pipeline/orphans.clj, pipeline/ownership.clj, promote/plan.clj

Every component carries x-source: generated|hand so a reader knows which is which.
"""
import json, re, copy, yaml, pathlib

HERE = pathlib.Path(__file__).parent
gen = json.load(open(HERE / "staging.components.generated.json"))
tool = json.load(open(HERE / "staging.tool.generated.json"))

STACK_NAMES = ["CapabilityId", "Version", "VersionRange", "RunId", "ConsumerId", "Resources", "Provide", "Need",
               "Provides", "Needs", "Stack", "Instance", "Resolution", "Wiring", "ReportFormat", "Suite"]

def G(name, src="malli: hive-compose.schema/%s @ origin/staging", table=gen):
    s = copy.deepcopy(table[name])
    s["x-source"] = "generated"
    s["x-malli"] = src % name if "%s" in src else src
    return s

def ref(n): return {"$ref": "#/components/schemas/" + n}
def obj(props, required=(), extra=None, desc=None):
    o = {"type": "object", "properties": props}
    if required: o["required"] = list(required)
    if extra is not None: o["additionalProperties"] = extra
    if desc: o["description"] = desc
    o["x-source"] = "hand"
    return o
S = {"type": "string"}; I = {"type": "integer"}; B = {"type": "boolean"}; N = {"type": "number"}
def arr(x): return {"type": "array", "items": x}
def nullable(x): return {"anyOf": [x, {"type": "null"}]}
ANY = {}

components = {}
# ---------------------------------------------------------------- generated value objects
for n in gen: components[n] = G(n)
for n in STACK_NAMES:
    components[n] = G(n, "malli: hive-compose.schema/%s @ origin/staging")

# ---------------------------------------------------------------- request: generated from the tool inputSchema
props = tool["inputSchema"]["properties"]
req_props = {}
for p, spec in props.items():
    if p == "command": continue
    s = copy.deepcopy(spec)
    if p == "env":   # the tool says only {type: object}; the call is checked against schema/Env (addon/checked-options)
        # a null value forgets that var from the section's remembered call env
        s = {"description": spec["description"], "type": "object",
             "additionalProperties": {"anyOf": [ref("EnvValue"), {"type": "null"}]}}
    req_props[p] = s
req_props["agent_id"] = {"type": "string", "description": "stamped by the MCP host on every call; claim/release fall back to it when agent is absent (addon/agent-param)"}
components["ComposeRequest"] = {
    "type": "object", "properties": req_props, "additionalProperties": True,
    "description": "The MCP `compose` tool params minus `command` (the op is the path segment / hive_call op). "
                   "Booleans also accept the strings \"true\"/\"false\" on the MCP side (addon/flag); HTTP clients should send booleans. "
                   "Over MCP unknown keys are ignored; each Req_<command> schema below is closed (additionalProperties false) so a client learns of a typo, "
                   "while the server still ignores what it does not read. A declared param of the wrong JSON type is answered 400 compose/invalid-param.",
    "x-source": "generated", "x-from": "hive-compose.addon/tool :inputSchema @ origin/staging"}

SUBJECT = ["up", "switch", "down", "stop", "restart", "ps", "logs", "claim", "release"]
def request_for(cmd):
    ps = tool["command-params"][cmd]  # hive-compose.addon/command-params: what the API enforces
    if not ps:
        return {"type": "object", "additionalProperties": True, "description": "takes no params", "x-source": "generated"}
    s = {"type": "object", "properties": {p: copy.deepcopy(req_props[p]) for p in ps},
         "additionalProperties": False, "x-source": "generated",
         "x-params-from": "hive-compose.addon/command-params (the API refuses any other key)"}
    if cmd in SUBJECT:
        # Not anyOf [{required:[target]},{required:[profile]}]: openapi-typescript 7.13 turns each
        # required-only branch into `unknown`, collapsing the whole body type (NOTES issue 1).
        s["x-requires-one-of"] = ["target", "profile"]
        s["description"] = "target resolves a section; else profile looks up a preset or active section id (ops/subject). Neither: missing-param."
    return s

# ---------------------------------------------------------------- envelope (hive-cabi/v1)
components["Ok"] = obj({"ok": {"const": True}, "value": ANY}, ["ok", "value"],
                       desc="hive-cabi/v1 success envelope (hive-spi.native.schema/Envelope). value is the Result's :ok payload.")
components["ErrorEnvelope"] = obj({
    "ok": {"const": False},
    "error": {"anyOf": [ref("ErrorCategory"), S], "description": "the Result category, QUALIFIED on the API wire: compose/unknown-profile "
              "(decision coordinator:45a1f993). The MCP tool text keeps the bare name (unknown-profile). Every nested :error keyword "
              "(e.g. an ownership-check or a program report) is qualified the same way (hive-compose.api/qualify-errors)."},
    "error/type": {"type": "string", "enum": ["native/unavailable", "native/timeout", "native/malformed", "native/failed"],
                   "description": "host-side transport failure only (hive_call); absent for compose errors"},
    "hint": S, "message": S}, ["ok", "error"], extra=True,
    desc="hive-cabi/v1 failure envelope. Every other key of the r/err map rides alongside (param, profile, known, stderr ...).")
# Derived, never listed: every literal (r/err :ns/name ..) under src/, the same scan
# openapi_contract_test runs, so a category added in source shows up here on the next run.
components["ErrorCategory"] = {"type": "string", "x-source": "generated",
  "x-from": "hive-compose.api/source-categories over src/ (via gen.clj); HTTP status per category: hive-compose.api/status-by-category",
  "enum": tool["categories"]}

# ---------------------------------------------------------------- hand-written response rows
components["ProgramReport"] = obj({"id": S, "pid": I, "log": S, "dir": S, "kind": S, "started-at": I, "nrepl-port": I,
    "started?": B, "reused?": B, "external?": B, "exited?": B, "status": {"enum": ["running", "exited"]},
    "exit-code": I, "log-tail": arr(S), "advice": S, "error": ref("ErrorEnvelope")}, ["id"], extra=False,
    desc="ops/start-programs! report row: started, reused, external (its nREPL already answered), exited within the grace, or error.")
components["ProgramRow"] = obj({"id": S, "pid": I, "kind": S, "log": S, "dir": S, "alive?": B,
    "status": {"enum": ["running", "exited"]}, "exited?": B, "exit-code": I, "log-tail": arr(S), "advice": S,
    "nrepl-port": I, "nrepl-ready?": B}, ["id"], desc="ops/program-row")
components["StepReport"] = obj({"id": ref("ProfileId"), "project": S, "action": {"enum": ["stop", "down"]},
    "services": arr(S), "ok?": B, "error": ref("ErrorEnvelope"), "kept": {"type": "object", "additionalProperties": arr(S),
    "description": "{service [section-id]} left up because another section needs it"}, "note": S,
    "unknown": arr(S), "by": {"enum": ["container-name"]}, "missing-env": S,
    "programs": arr(obj({"id": S, "pid": I, "ok?": B, "error": ref("ErrorEnvelope")}))},
    ["id", "project", "action", "services"], desc="plan Release step + ops/step-report")
guard = {"orphans-overridden": arr(obj({"pid": I, "name": S, "holds": {"type": "object"}})),
         "orphan-check": obj({"error": S, "message": S, "category": S}),
         "shared-overridden": arr(obj({"section": S, "agents": arr(S), "services": arr(S), "reason": {"enum": ["recreate", "config-differs"]},
                                       "outcome": {"type": "object", "description": "per shared service what the engine did, from container ids before and after the call", "additionalProperties": {"enum": ["recreated", "kept", "created", "absent"]}},
                                       "outcome-unknown": B})),
         "ownership-check": obj({"error": S, "message": S, "category": S}),
         "holder-skipped": obj({"section": S, "dir": S, "reason": S},
                               desc="the section holding these services was not used: its dir no longer exists; the call ran from the project defaults")}
components["UpResult"] = obj(dict({"profile": S, "project": S, "services": arr(S), "out": S, "needs": arr(S),
    "programs": arr(ref("ProgramReport")), "announced": B,
    "released": {"description": "switch only: what the previous current section alone needed", **arr(ref("StepReport"))}}, **guard),
    ["profile", "project", "services"], desc="ops/bring-up! (+ switch :released, addon/announced, orphan and ownership guards)")
components["DownResult"] = {"allOf": [ref("StepReport"), obj({"out": S})], "x-source": "hand", "description": "ops/down-profile!"}
components["RestartResult"] = obj(dict({"profile": S, "project": S, "services": arr(S),
    "action": {"enum": ["restart", "recreate"]}, "out": S, "note": S}, **guard), ["profile", "project", "services", "action"])
components["TouchResult"] = {"oneOf": [obj({"profile": S, "last-touch": I}, ["profile", "last-touch"]),
                                       obj({"touched": arr(S), "last-touch": I}, ["touched", "last-touch"])], "x-source": "hand"}
components["PsResult"] = obj({"profile": S, "project": S, "needs": arr(S), "running": arr(S)}, ["profile", "project", "needs", "running"])
components["LogsResult"] = obj({"out": S}, ["out"])
components["UsageReason"] = obj({"claim": S, "wave?": B, "until": I, "agent": S, "port": I, "peers": I, "service": S, "cpu": N,
    "source": {"enum": ["nrepl", "published", "cpu"]}}, desc="promote.swarm/usage reason: {claim} | {agent} | {port peers source} | {service cpu source}")
components["StatusRow"] = obj({"id": S, "project": S, "active?": B, "section?": B, "ttl-minutes": I,
    "idle-action": ref("IdleAction"), "activity": arr(S), "description": S, "config-ok?": B, "config-project": S,
    "dir": S, "files": arr(S), "env-keys": arr(S), "call-env-keys": arr(S),
    "inputs-from": {"type": "string", "description": "the section whose dir, files and env this target section runs from"},
    "inputs": {"enum": ["assumed"], "description": "adopted running with nothing saying how it was started: dir, files and env are the preset's or project's, never trusted as a holder"},
    "env-src": {"type": "object", "additionalProperties": S},
    "services": arr(S), "idle-seconds": I, "reap-in-seconds": I, "reap-blocked?": B,
    "claims": {"type": "object", "additionalProperties": obj({"note": S, "since": I, "seen": I, "wave?": B, "until": I, "expires-in-seconds": I})},
    "orphaned?": B, "usage": ref("Usage"), "in-use-via": arr(S), "programs": arr(ref("ProgramRow"))},
    ["id", "project", "active?", "ttl-minutes", "idle-action", "activity"])
components["ReaperPass"] = {"oneOf": [
    obj({"reconciled": arr(S), "reaped": arr(ref("StepReport")), "observed": ref("ObserveReport"),
         "adopted": arr(S), "at": I}, ["reconciled", "reaped"]),
    obj({"skipped": {"enum": ["not-initialized", "busy"]}}, ["skipped"]),
    obj({"error": S, "at": I}, ["error"])], "x-source": "hand", "description": "addon/tick!"}
components["ObserveReport"] = obj({"swarm": {"enum": ["ok", "absent", "off"]}, "agents": I, "in-use": arr(S),
    "expired": arr(obj({"section": S, "agent": S, "reason": {"enum": ["gone", "stale", "expired"]}})), "orphaned": arr(S),
    "notified": arr(obj({"agent": S, "section": S, "channel": {"enum": ["direct", "broadcast"]}}))},
    desc="pipeline.swarm/observe!")
components["StatusResult"] = obj({"ok?": B,
    "config": {"type": "object", "additionalProperties": obj({"compose-project": S, "ok?": B, "error": {"type": "object"}})},
    "current": nullable(S), "projects": arr(S), "profiles": arr(ref("StatusRow")), "problem": S,
    "last-pass": nullable(ref("ReaperPass"))}, ["ok?", "config", "projects", "profiles"], desc="ops/status + :last-pass")
components["TargetsResult"] = arr(obj({"project": S, "always-on": arr(S), "profiles": arr(S)}, ["project", "always-on", "profiles"]))
components["ProgramsResult"] = arr(obj({"project": S, "id": S, "dir": S, "with": arr(S), "running?": B, "kind": S,
    "command": {"anyOf": [S, arr(S)]}, "nrepl-port": I, "error": ref("ErrorEnvelope"), "pid": I, "log": S, "alive?": B,
    "nrepl-ready?": B}, ["project", "id", "dir", "with", "running?"]))
components["Route"] = obj({"dir": S, "id": S, "project": S, "compose-project": S, "nrepl-port": I, "running?": B,
    "nrepl-ready?": B, "sections": arr(S), "pid": I, "external?": B, "kind": S, "advice": S, "path": S},
    ["dir", "id", "project", "compose-project", "nrepl-port", "running?", "nrepl-ready?", "sections"], desc="pipeline.swarm/route + advice")
components["ClaimResult"] = obj({"profile": S, "agent": S, "claims": arr(S), "claim-ttl-minutes": I, "renew": S,
    "claim": obj({"note": S, "since": I, "seen": I, "wave?": B, "until": I, "expires-in-seconds": I}),
    "nrepl": {"description": "every route with advice on a first claim or an adoption; on a renewal only the running ones", **arr(ref("Route"))},
    "nrepl-not-running": {"type": "integer", "description": "renewal only: how many routes of the section do not run (left out of nrepl)"},
    "nrepl-hint": {"type": "string", "description": "renewal only: how to get the route of a module that does not run"},
    "started": ref("UpResult"),
    "renewed": obj({"profile": S, "services": arr(S)}, desc="the section was active: nothing was started, recreated or restarted"),
    "adopted": obj({"profile": S, "services": arr(S), "programs": arr(ref("ProgramReport")),
                    "programs-not-started": arr(ref("ProgramReport"))},
                   desc="the section was not tracked but all its services ran: taken without an up; its host programs started (host only), those that could not named under programs-not-started"),
    "wave-ended": {"type": "string", "description": "the caller's wave claim had passed its until: this renewal without ttl_minutes holds it as a plain claim"}}, ["profile", "agent", "claims", "claim-ttl-minutes", "renew", "nrepl"])
components["ReleaseResult"] = obj({"profile": S, "agent": S, "claims": arr(S), "orphaned?": B, "reap-in-seconds": I,
    "note": S, "stopped": ref("DownResult")}, ["profile", "agent", "claims"])
components["WhoResult"] = obj({"sections": arr(obj({"id": S, "project": S,
        "claims": {"type": "object", "additionalProperties": obj({"note": S, "since": I, "seen": I, "wave?": B, "until": I, "expires-in-seconds": I})},
        "in-use?": B, "used-by": arr(ref("UsageReason")), "in-use-via": arr(S), "activity": arr(S),
        "idle-seconds": I, "reap-in-seconds": I, "orphaned?": B, "exempt?": B})),
    "agents": arr(obj({"id": S, "status": S, "cwd": S, "sections": arr(S)}, extra=True)),
    "swarm": {"enum": ["absent"]}}, ["sections"])
components["NreplResult"] = {"oneOf": [
    {"allOf": [ref("Route"), obj({"path": S}, ["path"])]},
    obj({"path": S, "route": {"type": "null"}, "advice": S}, ["path", "route", "advice"]),
    obj({"routes": arr(ref("Route"))}, ["routes"])], "x-source": "hand"}
components["ProjectsResult"] = arr(obj({"name": S, "status": S, "managed?": B, "label-files": arr(S),
    "configured": arr(obj({"id": S, "files": arr(S)})), "stale-files": arr(S), "files-match?": B}, ["name", "managed?", "label-files"]))
keyed = obj({"added": arr(S), "removed": arr(S), "changed": arr(S)})
components["ReloadResult"] = obj({"source": {"enum": ["config-source", "host-config", "none"]}, "profiles": arr(S),
    "projects": arr(S), "changed": obj({"projects": keyed, "profiles": keyed, "settings": arr(S), "restart-required": arr(S)}),
    "sections": obj({"rebased": arr(S), "kept": arr(obj({"id": S, "reason": {"enum": ["project-removed"]}}))}),
    "not-reloaded": arr(S), "note": S,
    "call-env-dropped": {"type": "object", "description": "{section [var]}: remembered call env forgotten because the reload changed its declared value",
                         "additionalProperties": arr(S)}},
    ["source", "profiles", "projects", "changed", "sections", "not-reloaded"])
components["HelpResult"] = obj({"commands": {"type": "object", "additionalProperties": S}}, ["commands"])
components["AdoptResult"] = obj({"adopted": arr(S)}, ["adopted"])

RESULT = {"help": "HelpResult", "status": "StatusResult", "targets": "TargetsResult", "programs": "ProgramsResult",
          "up": "UpResult", "switch": "UpResult", "down": "DownResult", "stop": "DownResult", "restart": "RestartResult",
          "touch": "TouchResult", "ps": "PsResult", "logs": "LogsResult", "claim": "ClaimResult", "release": "ReleaseResult",
          "who": "WhoResult", "nrepl": "NreplResult", "reap": "ReaperPass", "adopt": "AdoptResult",
          "projects": "ProjectsResult", "reload": "ReloadResult",
          "faults": "FaultsResult", "stack-up": "StackUpResult", "stack-down": "StackDownResult",
          "stack-status": "StackStatusResult", "plan": "PlanResult", "why": "WhyResult",
          "flaky": "FlakyResult", "test": "TestResult"}
# Commands staging added after phase 1 (hc-4/6/10/12). Their result maps are built in
# pipeline/stacks.clj, pipeline/why.clj and pipeline/faults.clj with no malli schema yet, so
# they are typed open here rather than hand-copied (NOTES: response shapes drift).
def open_result(desc): return {"type": "object", "additionalProperties": True, "description": desc, "x-source": "hand"}
components["FaultsResult"] = open_result("pipeline.faults/faults!: list {proxy-name Proxy}, add the toxic, remove {removed?}, reset {reset true}")
components["StackUpResult"] = open_result("pipeline.stacks/stack-up!: the run, its instances and consumers")
components["StackDownResult"] = open_result("pipeline.stacks/stack-down!: what was taken down and what was kept")
components["StackStatusResult"] = open_result("pipeline.stacks/stack-status: stack runs with their instances' and consumers' containers")
components["PlanResult"] = open_result("pipeline.stacks/plan: what stack-up would start, share or find running")
components["FlakyResult"] = open_result("pipeline.flaky/flaky: test cases ranked by intermittent failure, each flaky, regression, fixed or stable")
components["WhyResult"] = open_result("pipeline.why/why! minus :why/markdown: case, error span, span path, logs, plan, written files")
components["TestResult"] = open_result("pipeline.suites/test!: run, stack, status (passed|failed|error), exit (0 every cell passed, 1 a test failed, 2 something could not run; 2 wins), summary, one cell per suite x project (x variant: suite/id <suite>@<variant>, variant; the variant env is set on the suite RUNNER only, the stack and its consumers are NOT restarted or re-env'd per variant; a suite without variants under a variants filter ran once and carries variant-note), each with status, reason when it did not pass (error: could not run, e.g. report/none, report/empty, a runner category) and service when known, report cut to totals, every case {case/id case/status case/time-s} and the failed cases with their texts; skip-fails?, stack-kept?, claimed-by, summary-file")
STACKS = {"faults", "stack-up", "stack-down", "stack-status", "plan", "why", "flaky", "test"}
MUTATES = {"up", "switch", "down", "stop", "restart", "touch", "claim", "release", "reap", "adopt", "reload",
           "faults", "stack-up", "stack-down", "why", "test"}
assert set(RESULT) == set(tool["commands"]), set(tool["commands"]) ^ set(RESULT)

def ok_of(schema_ref):
    return {"allOf": [ref("Ok"), {"type": "object", "properties": {"value": schema_ref}, "required": ["value"]}]}

responses = {
  "BadRequest": {"description": "compose/missing-param, compose/invalid-param (also a body that is not a JSON object)", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "NotFound": {"description": "compose/unknown-command, compose/unknown-profile, compose/unknown-target, compose/unknown-project, compose/no-projects", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "Conflict": {"description": "compose/not-active, compose/not-claimed, compose/orphans-hold-resources, compose/held-by-another-section (pass allow_orphans / allow_shared), compose/holders-disagree (pass profile=<id>)", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "EngineFailed": {"description": "compose/command-failed (docker compose exited non-zero; stderr scrubbed and clipped), compose/secret-unresolved, secrets/unknown-backend", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "Forbidden": {"description": "compose/forbidden-origin: Host not a loopback name, or an Origin header from a web page", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "UnsupportedMediaType": {"description": "compose/unsupported-media-type: a POST without Content-Type application/json", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "Internal": {"description": "compose/dispatch-threw, compose/internal and any unmapped category (class and message only)", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "Unavailable": {"description": "compose/not-initialized (addon not active), compose/invalid-config", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
}

paths = {}
for cmd in tool["commands"]:
    rq = request_for(cmd)
    rname = "Req_" + cmd
    components[rname] = rq
    op = {"operationId": cmd, "summary": tool["command-help"][cmd].split(".")[0][:110],
          "description": tool["command-help"][cmd],
          "tags": ["swarm" if cmd in ("claim", "release", "who", "nrepl") else "stacks" if cmd in STACKS else "sections"],
          "x-hive-call-op": cmd, "x-mcp": {"tool": "compose", "command": cmd}, "x-mutates": cmd in MUTATES,
          "requestBody": {"required": False, "content": {"application/json": {"schema": ref(rname)}}},
          "responses": {"200": {"description": "Result ok", "content": {"application/json": {"schema": ok_of(ref(RESULT[cmd]))}}},
                        "400": {"$ref": "#/components/responses/BadRequest"},
                        "403": {"$ref": "#/components/responses/Forbidden"},
                        "415": {"$ref": "#/components/responses/UnsupportedMediaType"},
                        "500": {"$ref": "#/components/responses/Internal"}}}
    if cmd != "help":  # help answers before init
        op["responses"]["503"] = {"$ref": "#/components/responses/Unavailable"}
    if cmd in SUBJECT or cmd in ("touch",) or cmd in STACKS:
        op["responses"]["404"] = {"$ref": "#/components/responses/NotFound"}
    if cmd in ("up", "switch", "restart", "touch", "release", "claim") or cmd in STACKS:
        op["responses"]["409"] = {"$ref": "#/components/responses/Conflict"}
    if cmd in SUBJECT or cmd in ("targets", "projects", "reload") or cmd in STACKS:
        op["responses"]["502"] = {"$ref": "#/components/responses/EngineFailed"}
    paths["/v1/" + cmd] = {"post": op}

# ---------------------------------------------------------------- planned stack ops (placeholders)
PLANNED = []  # hc-8 `test` is live since feat/hc8-pipeline

components["Report"] = dict(obj({"format": {"enum": ["junit", "tap"]},
    "cases": arr(obj({"case/id": S, "name": S, "classname": S, "status": {"enum": ["passed", "failed", "error", "skipped"]},
                      "time-s": N, "message": S, "output": S}, ["case/id", "name", "status"])),
    "totals": obj({"passed": I, "failed": I, "error": I, "skipped": I})}, ["format", "cases", "totals"],
    desc="hc-8 Report (promote/report.clj); hand-copied"),
    **{"x-source": "hand"})
REQ_REQUIRED = {}
for path, op, card, desc, reqp, res in PLANNED:
    rn = "Req_" + op.replace(".", "_"); vn = "Res_" + op.replace(".", "_")
    components[rn] = dict(obj(reqp, REQ_REQUIRED.get(op, ())), **{"x-status": "planned", "x-params-from": "hc-lane-core provisional hc-6 params (hivemind 2026-11-22)" if op.startswith("stack.") else "plan step description"}); components[vn] = dict(res, **{"x-status": "planned"})
    paths["/v1/" + path] = {"post": {"operationId": op.replace(".", "_"), "summary": desc.split(".")[0][:110], "description": desc,
        "tags": ["stacks (planned)"], "x-status": "planned", "x-plan-step": card, "x-hive-call-op": op,
        "requestBody": {"content": {"application/json": {"schema": ref(rn)}}},
        "responses": {"200": {"description": "Result ok", "content": {"application/json": {"schema": ok_of(ref(vn))}}},
                      "400": {"$ref": "#/components/responses/BadRequest"},
                      "501": {"description": "not implemented yet (planned)", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}}}}}

components["HealthResult"] = obj({"status": {"enum": ["ok", "degraded", "down"]}, "details": {"type": "object"}}, ["status"],
                                desc="IAddon health of hive.compose")
paths["/v1/health"] = {"get": {"operationId": "health", "summary": "addon health (IAddon health)", "tags": ["meta"],
    "responses": {"200": {"description": "health", "content": {"application/json": {"schema": ok_of(ref("HealthResult"))}}}}}}
paths["/v1/openapi.yaml"] = {"get": {"operationId": "openapi", "summary": "this document", "tags": ["meta"],
    "responses": {"200": {"description": "the OpenAPI document", "content": {"application/yaml": {"schema": {"type": "string"}}}}}}}

doc = {
  "openapi": "3.1.0",
  "info": {"title": "hive-compose control API (hc-14)", "version": "0.1.0",
           "license": {"name": "MIT", "identifier": "MIT"},
           "description": "Mirror of the `compose` MCP tool (hive.compose addon) as HTTP/JSON. One op per command: "
             "POST /v1/{command} with a JSON body equal to the MCP params minus `command`. The same (op, body) is what "
             "hive_call(op, json) takes over the hive C ABI; both answer the hive-cabi/v1 envelope "
             "{ok:true,value} | {ok:false,error,...}. Generated parts carry x-source: generated (malli.json-schema of "
             "hive-compose.schema and the tool's live inputSchema); response shapes are hand-written from pipeline/*.clj. "
             "Paths tagged `stacks (planned)` / x-status: planned are placeholders for plan 20261122140443-09c5c225 and answer 501 "
             "compose/not-implemented. Off by default: config :compose/api {:enabled? true :port 0 :host \"127.0.0.1\"}; "
             "`status` answers the bound address under `api`."},
  "jsonSchemaDialect": "https://spec.openapis.org/oas/3.1/dialect/base",
  "servers": [{"url": "http://127.0.0.1:{port}", "variables": {"port": {"default": "0", "description": ":compose/api :port (0 binds an ephemeral port; the compose status command answers api.port)"}}}],
  "tags": [{"name": "sections", "description": "existing section commands"}, {"name": "swarm", "description": "existing claim / nREPL routing commands"},
           {"name": "stacks", "description": "stack runs, fault injection, failure explanation and flaky ranking (hc-4/6/10/12/13)"},
           {"name": "meta", "description": "health and this document"}],
  "security": [],
  "x-trust-model": "No authentication. The server binds a loopback address only and refuses browser traffic (Host must be a loopback name, "
                   "an Origin header must be loopback, POST must be application/json so a cross-site request needs a preflight that is never granted). "
                   "Every local process is trusted as the operator: `agent`/`agent_id` are taken as sent, so any local caller can claim or release for any agent.",
  "paths": paths,
  "components": {"schemas": components, "responses": responses},
}

class D(yaml.SafeDumper): pass
D.ignore_aliases = lambda *a: True
out = HERE.parent.parent.parent / "resources" / "hive_compose" / "openapi.yaml"
out.parent.mkdir(parents=True, exist_ok=True)
with open(out, "w") as f:
    f.write("# hc-14 (card 20261122140510-06e82712). GENERATED by doc/api/gen/assemble.py; do not hand-edit, edit the script.\n")
    yaml.dump(doc, f, Dumper=D, sort_keys=False, width=110, allow_unicode=True)
print("wrote", out, "paths", len(paths), "schemas", len(components))
