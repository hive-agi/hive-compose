#!/usr/bin/env python3
"""Assemble resources/hive_compose/openapi.yaml (hc-14; served at GET /v1/openapi.yaml) from:

  GENERATED  staging.components.generated.json  malli.json-schema of hive-compose.schema @ origin/staging
  GENERATED  hc2.components.generated.json      malli.json-schema of the hc-2 stack schemas (uncommitted worktree)
  GENERATED  staging.tool.generated.json        the MCP tool's live :inputSchema + command-help
  HAND       RESPONSES / ERRORS / PLANNED below, read off pipeline/ops.clj, pipeline/swarm.clj,
             pipeline/restart.clj, pipeline/orphans.clj, pipeline/ownership.clj, promote/plan.clj

Every component carries x-source: generated|hand so a reader knows which is which.
"""
import json, re, copy, yaml, pathlib

HERE = pathlib.Path(__file__).parent
gen = json.load(open(HERE / "staging.components.generated.json"))
gen_hc2 = json.load(open(HERE / "hc2.components.generated.json"))
tool = json.load(open(HERE / "staging.tool.generated.json"))

STACK_NAMES = ["CapabilityId", "Version", "VersionRange", "RunId", "ConsumerId", "Provide", "Need",
               "Provides", "Needs", "Stack", "Instance", "Resolution", "Wiring", "ReportFormat", "Suite"]

def G(name, src="malli: hive-compose.schema/%s @ origin/staging db09adc", table=gen):
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
    components[n] = G(n, "malli: hive-compose.schema/%s @ worktree hc-2-schema (feat/hc-2-stack-schema, UNCOMMITTED)", gen_hc2)
    components[n]["x-status"] = "planned"

# ---------------------------------------------------------------- request: generated from the tool inputSchema
props = tool["inputSchema"]["properties"]
applies = {}  # command -> [param]
for p, spec in props.items():
    if p == "command": continue
    for m in re.finditer(r"\[([a-z|_]+)\]", spec.get("description", "")):
        for c in m.group(1).split("|"):
            applies.setdefault(c, []).append(p)
req_props = {}
for p, spec in props.items():
    if p == "command": continue
    s = copy.deepcopy(spec)
    if p == "env":   # the tool says only {type: object}; the call is checked against schema/Env (addon/checked-options)
        s = {"description": spec["description"], "allOf": [ref("Env")]}
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
    ps = sorted(set(applies.get(cmd, [])) | ({"target", "profile"} if cmd in SUBJECT + ["touch"] else set())
                | ({"agent_id"} if cmd in ("claim", "release") else set()))
    if not ps:
        return {"type": "object", "additionalProperties": True, "description": "takes no params", "x-source": "generated"}
    s = {"type": "object", "properties": {p: copy.deepcopy(req_props[p]) for p in ps},
         "additionalProperties": False, "x-source": "generated",
         "x-params-from": "the [cmd|..] prefixes of the tool's param descriptions"}
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
components["ErrorCategory"] = {"type": "string", "x-source": "hand", "x-from": "carto structural-grep (r/err ?cat ?&rest) over src/ @ origin/staging",
  "enum": ["compose/not-initialized", "compose/unknown-command", "compose/missing-param", "compose/invalid-param",
           "compose/unknown-profile", "compose/unknown-project", "compose/unknown-target", "compose/no-projects",
           "compose/not-active", "compose/not-claimed", "compose/command-failed", "compose/secret-unresolved",
           "compose/invalid-config", "compose/program-needs-command", "compose/orphans-hold-resources",
           "compose/held-by-another-section", "compose/orphan-check-failed", "compose/ownership-check-failed",
           "compose/release-threw", "compose/notice-failed", "compose/swarm-failed", "compose/notice-dropped",
           "compose/dispatch-threw", "compose/not-implemented", "compose/unknown-route", "compose/method-not-allowed",
           "compose/payload-too-large", "compose/forbidden-origin", "compose/unsupported-media-type",
           "compose/internal", "compose/proc-unreadable", "compose/api-bind-failed", "secrets/unknown-backend"]}

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
         "shared-overridden": arr(obj({"section": S, "agents": arr(S), "services": arr(S), "reason": {"enum": ["recreate", "config-differs"]}})),
         "ownership-check": obj({"error": S, "message": S, "category": S})}
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
components["UsageReason"] = obj({"claim": S, "agent": S, "port": I, "peers": I, "service": S, "cpu": N,
    "source": {"enum": ["nrepl", "published", "cpu"]}}, desc="promote.swarm/usage reason: {claim} | {agent} | {port peers source} | {service cpu source}")
components["StatusRow"] = obj({"id": S, "project": S, "active?": B, "section?": B, "ttl-minutes": I,
    "idle-action": ref("IdleAction"), "activity": arr(S), "description": S, "config-ok?": B, "config-project": S,
    "services": arr(S), "idle-seconds": I, "reap-in-seconds": I, "reap-blocked?": B,
    "claims": {"type": "object", "additionalProperties": obj({"note": S, "since": I, "seen": I})},
    "orphaned?": B, "usage": ref("Usage"), "in-use-via": arr(S), "programs": arr(ref("ProgramRow"))},
    ["id", "project", "active?", "ttl-minutes", "idle-action", "activity"])
components["ReaperPass"] = {"oneOf": [
    obj({"reconciled": arr(S), "reaped": arr(ref("StepReport")), "observed": ref("ObserveReport"),
         "adopted": arr(S), "at": I}, ["reconciled", "reaped"]),
    obj({"skipped": {"enum": ["not-initialized", "busy"]}}, ["skipped"]),
    obj({"error": S, "at": I}, ["error"])], "x-source": "hand", "description": "addon/tick!"}
components["ObserveReport"] = obj({"swarm": {"enum": ["ok", "absent", "off"]}, "agents": I, "in-use": arr(S),
    "expired": arr(obj({"section": S, "agent": S, "reason": {"enum": ["gone", "stale"]}})), "orphaned": arr(S),
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
    "nrepl": arr(ref("Route")), "started": ref("UpResult")}, ["profile", "agent", "claims", "claim-ttl-minutes", "renew", "nrepl"])
components["ReleaseResult"] = obj({"profile": S, "agent": S, "claims": arr(S), "orphaned?": B, "reap-in-seconds": I,
    "note": S, "stopped": ref("DownResult")}, ["profile", "agent", "claims"])
components["WhoResult"] = obj({"sections": arr(obj({"id": S, "project": S,
        "claims": {"type": "object", "additionalProperties": obj({"note": S, "since": I, "seen": I})},
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
    "not-reloaded": arr(S), "note": S}, ["source", "profiles", "projects", "changed", "sections", "not-reloaded"])
components["HelpResult"] = obj({"commands": {"type": "object", "additionalProperties": S}}, ["commands"])
components["AdoptResult"] = obj({"adopted": arr(S)}, ["adopted"])

RESULT = {"help": "HelpResult", "status": "StatusResult", "targets": "TargetsResult", "programs": "ProgramsResult",
          "up": "UpResult", "switch": "UpResult", "down": "DownResult", "stop": "DownResult", "restart": "RestartResult",
          "touch": "TouchResult", "ps": "PsResult", "logs": "LogsResult", "claim": "ClaimResult", "release": "ReleaseResult",
          "who": "WhoResult", "nrepl": "NreplResult", "reap": "ReaperPass", "adopt": "AdoptResult",
          "projects": "ProjectsResult", "reload": "ReloadResult"}
MUTATES = {"up", "switch", "down", "stop", "restart", "touch", "claim", "release", "reap", "adopt", "reload"}
assert set(RESULT) == set(tool["commands"]), set(tool["commands"]) ^ set(RESULT)

def ok_of(schema_ref):
    return {"allOf": [ref("Ok"), {"type": "object", "properties": {"value": schema_ref}, "required": ["value"]}]}

responses = {
  "BadRequest": {"description": "compose/missing-param, compose/invalid-param (also a body that is not a JSON object)", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "NotFound": {"description": "compose/unknown-command, compose/unknown-profile, compose/unknown-target, compose/unknown-project, compose/no-projects", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
  "Conflict": {"description": "compose/not-active, compose/not-claimed, compose/orphans-hold-resources, compose/held-by-another-section (pass allow_orphans / allow_shared)", "content": {"application/json": {"schema": ref("ErrorEnvelope")}}},
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
          "description": tool["command-help"][cmd], "tags": ["swarm" if cmd in ("claim", "release", "who", "nrepl") else "sections"],
          "x-hive-call-op": cmd, "x-mcp": {"tool": "compose", "command": cmd}, "x-mutates": cmd in MUTATES,
          "requestBody": {"required": False, "content": {"application/json": {"schema": ref(rname)}}},
          "responses": {"200": {"description": "Result ok", "content": {"application/json": {"schema": ok_of(ref(RESULT[cmd]))}}},
                        "400": {"$ref": "#/components/responses/BadRequest"},
                        "403": {"$ref": "#/components/responses/Forbidden"},
                        "415": {"$ref": "#/components/responses/UnsupportedMediaType"},
                        "500": {"$ref": "#/components/responses/Internal"}}}
    if cmd != "help":  # help answers before init
        op["responses"]["503"] = {"$ref": "#/components/responses/Unavailable"}
    if cmd in SUBJECT or cmd in ("touch",):
        op["responses"]["404"] = {"$ref": "#/components/responses/NotFound"}
    if cmd in ("up", "switch", "restart", "touch", "release", "claim"):
        op["responses"]["409"] = {"$ref": "#/components/responses/Conflict"}
    if cmd in SUBJECT or cmd in ("targets", "projects", "reload"):
        op["responses"]["502"] = {"$ref": "#/components/responses/EngineFailed"}
    paths["/v1/" + cmd] = {"post": op}

# ---------------------------------------------------------------- planned stack ops (placeholders)
PLANNED = [
  ("stack/plan", "stack.plan", "hc-4-stack-plan", "Resolved graph to instances, compose -p names, start order; what is shared, private and already running (terraform-plan style). Lockfile of image digests. Pure, mutates nothing.",
   {"stack": S},
   obj({"stack": S, "resolution": ref("Resolution"),
        "instances": arr(obj({"instance": ref("Instance"), "state": {"enum": ["shared-running", "shared-new", "private-new", "running"]}})),
        "order": arr(S), "wiring": arr(ref("Wiring")), "lockfile": {"type": "object", "additionalProperties": S}})),
  ("stack/up", "stack.up", "hc-6-stack-ops", "Bring a stack up over its plan: run id, per-run network, docker labels hive.run/hive.stack, up --wait plus health gating of dependents.",
   {"stack": S, "run": ref("RunId"), "wait": B},
   obj({"stack": S, "run": ref("RunId"), "instances": arr(obj({"id": S, "compose-project": S, "started?": B, "healthy?": B})),
        "wiring": arr(ref("Wiring"))})),
  ("stack/down", "stack.down", "hc-6-stack-ops", "Release a stack run; instances other stacks still need are spared (refcount). Volumes never removed.",
   {"stack": S, "run": ref("RunId")},
   obj({"run": ref("RunId"), "released": arr(S), "kept": {"type": "object", "additionalProperties": arr(S)}})),
  ("stack/status", "stack.status", "hc-6-stack-ops", "Runs, instances with refcounts, claims and reaper countdown per stack.",
   {"stack": S, "run": ref("RunId")},
   obj({"runs": arr(obj({"run": ref("RunId"), "stack": S, "instances": arr(obj({"id": S, "refs": I, "healthy?": B}))}))})),
  ("test", "test", "hc-8-suites", "Expand suites x projects to the minimal union stack, run each runner (container or command) with TRACEPARENT and baggage test.id, collect exit code plus JUnit XML or TAP.",
   {"suites": arr(S), "projects": arr(S), "run": ref("RunId")},
   obj({"run": ref("RunId"), "cells": arr(obj({"suite": S, "project": S, "exit": I, "report": ref("Report")}))})),
  ("why", "why", "hc-10-why", "Failure bundle for test T: the case, traces carrying test.id (Tempo/ClickHouse behind a port), container logs in its window, the resolved plan; written to the state dir plus a memory note.",
   {"test": S, "run": ref("RunId")},
   obj({"test": S, "run": ref("RunId"), "case": {"type": "object"}, "spans": arr({"type": "object"}), "logs": {"type": "object", "additionalProperties": S},
        "plan": {"type": "object"}, "bundle": S, "memory-id": S})),
  ("flaky", "flaky", "hc-13-flaky", "Rank tests by intermittent failure across runs, so a flake is told apart from a regression.",
   {"suite": S, "limit": I, "since": S},
   obj({"tests": arr(obj({"id": S, "runs": I, "failures": I, "flips": I, "score": N, "verdict": {"enum": ["flaky", "regression", "stable"]}}))})),
  ("faults", "faults", "hc-12-faults", "Toxiproxy interposed between a consumer and an instance (:needs {\"pg\" {:via :toxiproxy}}): list, add or remove toxics.",
   {"run": ref("RunId"), "consumer": ref("ConsumerId"), "capability": ref("CapabilityId"),
    "action": {"enum": ["list", "add", "remove"]}, "toxic": obj({"type": {"enum": ["latency", "bandwidth", "timeout", "reset_peer", "slicer", "limit_data"]}, "attributes": {"type": "object"}, "stream": {"enum": ["upstream", "downstream"]}})},
   obj({"proxies": arr(obj({"name": S, "listen": S, "upstream": S, "toxics": arr({"type": "object"})}))})),
]
components["Report"] = dict(obj({"format": {"enum": ["junit", "tap"]},
    "cases": arr(obj({"case/id": S, "name": S, "classname": S, "status": {"enum": ["passed", "failed", "error", "skipped"]},
                      "time-s": N, "message": S, "output": S}, ["case/id", "name", "status"])),
    "totals": obj({"passed": I, "failed": I, "error": I, "skipped": I})}, ["format", "cases", "totals"],
    desc="hc-8 Report as relayed by hc-lane-core (branch feat/hc8-report, promote/report.clj, uncommitted); hand-copied, regenerate once merged"),
    **{"x-status": "planned"})
REQ_REQUIRED = {"stack.plan": ["stack"], "stack.up": ["stack"], "stack.down": ["stack", "run"], "why": ["test"]}
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
           {"name": "stacks (planned)", "description": "hc-4/6/8/10/12/13 placeholders"},
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
