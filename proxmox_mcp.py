# /// script
# requires-python = ">=3.10"
# dependencies = ["mcp>=1.10", "httpx>=0.27"]
# ///
"""
Proxmox VE MCP server: cluster/node/guest status and LXC container management.

Talks to the Proxmox REST API (port 8006) with an API token. Runs over stdio.

Run:
    uv run proxmox_mcp.py                 (uv installs dependencies itself)
  or
    pip install "mcp>=1.10" "httpx>=0.27" && python proxmox_mcp.py

Environment variables:
    PROXMOX_HOST          required. Host or comma-separated list of cluster nodes
                          to try in order, e.g. "pve1.lan,192.168.1.11".
                          Port 8006 and https are assumed unless given explicitly.
    PROXMOX_TOKEN_ID      required. "user@realm!tokenname", e.g. "claude@pve!mcp".
    PROXMOX_TOKEN_SECRET  required. The token's secret (UUID).
    PROXMOX_VERIFY_SSL    "true" to verify the TLS certificate. Default: false
                          (Proxmox ships with a self-signed certificate).
    PROXMOX_READ_ONLY     "true" to expose status tools only (nothing that changes state).
    PROXMOX_SSH_HOSTS     optional. Enables the container_exec tool (commands inside
                          a container via `ssh <target> pct exec`). Format:
                          "node1=root@192.168.1.10,node2=root@192.168.1.11".
                          Several targets per node, tried in order, are separated
                          with "|": "node2=root@192.168.1.11|root@100.64.0.2".
                          Needs key-based SSH from this machine to the node.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import time
from typing import Any

import httpx
from mcp.types import ToolAnnotations

try:                                    # mcp 2.x
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
except ImportError:                     # mcp 1.x, where the same class is called FastMCP
    from mcp.server.fastmcp import FastMCP as MCPServer
    from mcp.server.fastmcp.exceptions import ToolError


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _base_url(host: str) -> str:
    host = host.strip().rstrip("/")
    if not host.startswith(("http://", "https://")):
        host = "https://" + host
    scheme, rest = host.split("://", 1)
    if ":" not in rest.split("/", 1)[0]:
        rest = rest + ":8006"
    return f"{scheme}://{rest}"


HOSTS = [_base_url(h) for h in os.environ.get("PROXMOX_HOST", "").split(",") if h.strip()]
TOKEN_ID = os.environ.get("PROXMOX_TOKEN_ID", "").strip()
TOKEN_SECRET = os.environ.get("PROXMOX_TOKEN_SECRET", "").strip()
VERIFY_SSL = _bool_env("PROXMOX_VERIFY_SSL", False)
READ_ONLY = _bool_env("PROXMOX_READ_ONLY", False)

SSH_HOSTS: dict[str, str] = {}
for _pair in os.environ.get("PROXMOX_SSH_HOSTS", "").split(","):
    if "=" in _pair:
        _node, _target = _pair.split("=", 1)
        SSH_HOSTS[_node.strip()] = _target.strip()

logging.getLogger("httpx").setLevel(logging.WARNING)   # keep per-request lines out of the client log

TASK_WAIT_SECONDS = 60          # how long a tool waits for a Proxmox task before returning its UPID
MAX_EXEC_OUTPUT = 20_000        # characters of command output returned by container_exec

mcp = MCPServer(
    "proxmox",
    instructions=(
        "Tools for a Proxmox VE host or cluster. Start with cluster_overview or "
        "list_guests. Guests are addressed by numeric VMID; the node is resolved "
        "automatically. Long operations return a task UPID - check it with task_status. "
        "Before creating a container, check list_templates and list_storage for valid "
        "values; before creating a VM, list_isos and list_storage on the target node. "
        "Confirm with the user before stopping, rolling back or deleting anything."
    ),
)


def _annotations(read_only: bool, destructive: bool = False) -> ToolAnnotations:
    # validated by wire name so it works on both SDK generations
    return ToolAnnotations.model_validate({"readOnlyHint": read_only, "destructiveHint": destructive})


def read_tool(fn):
    """Register a tool that only reads state."""
    return mcp.tool(annotations=_annotations(True), structured_output=False)(fn)


def write_tool(destructive: bool = False):
    """Register a state-changing tool unless the server runs in read-only mode."""
    def decorator(fn):
        if READ_ONLY:
            return fn
        return mcp.tool(annotations=_annotations(False, destructive), structured_output=False)(fn)
    return decorator


# --------------------------------------------------------------------------- #
# API client
# --------------------------------------------------------------------------- #

class ProxmoxError(ToolError):
    """An expected failure. As a ToolError its message is passed on to the model
    (the SDK hides the text of any other exception)."""


_client: httpx.AsyncClient | None = None
_preferred_host: str | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    missing = [
        name for name, value in (
            ("PROXMOX_HOST", HOSTS), ("PROXMOX_TOKEN_ID", TOKEN_ID), ("PROXMOX_TOKEN_SECRET", TOKEN_SECRET)
        ) if not value
    ]
    if missing:
        raise ProxmoxError("Server is not configured: set " + ", ".join(missing))
    if _client is None:
        _client = httpx.AsyncClient(
            verify=VERIFY_SSL,
            trust_env=False,          # LAN API: never route through a system HTTP(S)_PROXY
            timeout=httpx.Timeout(30.0, connect=5.0),
            headers={"Authorization": f"PVEAPIToken={TOKEN_ID}={TOKEN_SECRET}"},
        )
    return _client


def _encode(value: Any) -> Any:
    if isinstance(value, bool):
        return 1 if value else 0
    return value


async def api(method: str, path: str, params: dict[str, Any] | None = None) -> Any:
    """Call the Proxmox API and return the `data` field. Tries each configured host in turn."""
    global _preferred_host
    client = _get_client()
    clean = {k: _encode(v) for k, v in (params or {}).items() if v is not None}
    kwargs = {"params": clean} if method in ("GET", "DELETE") else {"data": clean}

    hosts = sorted(HOSTS, key=lambda h: h != _preferred_host)
    last_error: Exception | None = None
    for base in hosts:
        try:
            response = await client.request(method, f"{base}/api2/json{path}", **kwargs)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            last_error = exc          # connection never opened, safe to try the next node
            continue
        except httpx.HTTPError as exc:
            raise ProxmoxError(f"{method} {path}: no valid answer from {base} ({type(exc).__name__}: {exc})")
        _preferred_host = base
        if response.status_code >= 400:
            raise ProxmoxError(_describe_error(method, path, response))
        return response.json().get("data")
    raise ProxmoxError(f"Cannot reach the Proxmox API at {', '.join(HOSTS)}: {last_error}")


def _describe_error(method: str, path: str, response: httpx.Response) -> str:
    detail = response.reason_phrase or ""
    try:
        body = response.json()
        if body.get("errors"):
            detail += " " + json.dumps(body["errors"], ensure_ascii=False)
        elif body.get("message"):
            detail += " " + str(body["message"])
    except Exception:
        pass
    hint = ""
    if response.status_code == 401:
        hint = " (check PROXMOX_TOKEN_ID / PROXMOX_TOKEN_SECRET)"
    elif response.status_code == 403:
        hint = " (the API token lacks the privilege for this call)"
    return f"{method} {path} failed: HTTP {response.status_code} {detail.strip()}{hint}"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _out(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=1, default=str)


def _gb(value: Any) -> float | None:
    return None if value is None else round(float(value) / 1024**3, 2)


def _pct(value: Any) -> float | None:
    return None if value is None else round(float(value) * 100, 1)


def _uptime(seconds: Any) -> str | None:
    if not seconds:
        return None
    seconds = int(seconds)
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes = rest // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _guest_row(r: dict[str, Any]) -> dict[str, Any]:
    row = {
        "vmid": r.get("vmid"),
        "name": r.get("name"),
        "type": r.get("type"),
        "node": r.get("node"),
        "status": r.get("status"),
        "cpu_pct": _pct(r.get("cpu")),
        "cpus": r.get("maxcpu"),
        "mem_gb": [_gb(r.get("mem")), _gb(r.get("maxmem"))],
        "disk_gb": [_gb(r.get("disk")), _gb(r.get("maxdisk"))],
        "uptime": _uptime(r.get("uptime")),
    }
    for key in ("tags", "lock"):
        if r.get(key):
            row[key] = r[key]
    if r.get("template"):
        row["template"] = True
    return row


async def _locate(vmid: int) -> dict[str, Any]:
    """Find which node a guest lives on and whether it is lxc or qemu."""
    for r in await api("GET", "/cluster/resources", {"type": "vm"}):
        if int(r.get("vmid", -1)) == int(vmid):
            return r
    raise ProxmoxError(f"No VM or container with ID {vmid}")


async def _locate_container(vmid: int) -> dict[str, Any]:
    guest = await _locate(vmid)
    if guest.get("type") != "lxc":
        raise ProxmoxError(f"{vmid} ({guest.get('name')}) is a {guest.get('type')} VM, not an LXC container")
    return guest


async def _locate_vm(vmid: int) -> dict[str, Any]:
    guest = await _locate(vmid)
    if guest.get("type") != "qemu":
        raise ProxmoxError(f"{vmid} ({guest.get('name')}) is an LXC container, not a VM")
    return guest


def _upid_node(upid: str) -> str:
    parts = upid.split(":")
    if len(parts) < 3 or parts[0] != "UPID":
        raise ProxmoxError(f"Not a task UPID: {upid!r}")
    return parts[1]


async def _task_log_tail(upid: str, lines: int) -> list[str]:
    node = _upid_node(upid)
    log = await api("GET", f"/nodes/{node}/tasks/{upid}/log", {"limit": 5000}) or []
    return [entry.get("t", "") for entry in log][-lines:]


async def _wait_task(result: Any, timeout: int = TASK_WAIT_SECONDS) -> dict[str, Any]:
    """Wait for a Proxmox task. `result` is whatever the API returned (a UPID for async calls)."""
    if not isinstance(result, str) or not result.startswith("UPID:"):
        return {"ok": True, "result": result}
    upid, node = result, _upid_node(result)
    deadline = time.monotonic() + timeout
    while True:
        status = await api("GET", f"/nodes/{node}/tasks/{upid}/status")
        if status.get("status") == "stopped":
            ok = status.get("exitstatus") == "OK"
            info: dict[str, Any] = {"ok": ok, "exitstatus": status.get("exitstatus"), "upid": upid}
            if not ok:
                info["log"] = await _task_log_tail(upid, 25)
            return info
        if time.monotonic() >= deadline:
            return {
                "ok": None,
                "state": "still running",
                "upid": upid,
                "note": "Check progress with task_status(upid).",
            }
        await asyncio.sleep(1)


# --------------------------------------------------------------------------- #
# Status tools (read-only)
# --------------------------------------------------------------------------- #

@read_tool
async def cluster_overview() -> str:
    """Summary of the whole Proxmox installation: version, cluster quorum, every node
    (online state, CPU, memory, uptime) and counts of VMs and containers."""
    version = await api("GET", "/version")
    status = await api("GET", "/cluster/status")
    resources = await api("GET", "/cluster/resources")

    cluster = next((s for s in status if s.get("type") == "cluster"), None)
    online = {s.get("name"): bool(s.get("online")) for s in status if s.get("type") == "node"}
    nodes = [
        {
            "node": r.get("node"),
            "online": online.get(r.get("node"), r.get("status") == "online"),
            "cpu_pct": _pct(r.get("cpu")),
            "cpus": r.get("maxcpu"),
            "mem_gb": [_gb(r.get("mem")), _gb(r.get("maxmem"))],
            "root_disk_gb": [_gb(r.get("disk")), _gb(r.get("maxdisk"))],
            "uptime": _uptime(r.get("uptime")),
        }
        for r in resources if r.get("type") == "node"
    ]
    guests = [r for r in resources if r.get("type") in ("lxc", "qemu") and not r.get("template")]
    return _out({
        "version": version.get("version"),
        "cluster": (
            {"name": cluster.get("name"), "quorate": bool(cluster.get("quorate")), "nodes": cluster.get("nodes")}
            if cluster else "standalone node (no cluster)"
        ),
        "nodes": sorted(nodes, key=lambda n: str(n["node"])),
        "guests": {
            "containers": sum(1 for g in guests if g["type"] == "lxc"),
            "vms": sum(1 for g in guests if g["type"] == "qemu"),
            "running": sum(1 for g in guests if g.get("status") == "running"),
            "stopped": sum(1 for g in guests if g.get("status") != "running"),
        },
    })


@read_tool
async def list_guests(node: str | None = None, kind: str | None = None, running_only: bool = False) -> str:
    """List VMs and LXC containers with status and resource usage.

    node: only guests on this node. kind: "lxc" or "qemu". running_only: skip stopped guests.
    mem_gb and disk_gb are [used, total]."""
    rows = []
    for r in await api("GET", "/cluster/resources", {"type": "vm"}):
        if node and r.get("node") != node:
            continue
        if kind and r.get("type") != kind:
            continue
        if running_only and r.get("status") != "running":
            continue
        rows.append(_guest_row(r))
    return _out(sorted(rows, key=lambda g: int(g["vmid"] or 0)))


@read_tool
async def guest_details(vmid: int) -> str:
    """Full details of one VM or container: live status, complete configuration and,
    when it is running, its network interfaces and IP addresses."""
    guest = await _locate(vmid)
    node, kind = guest["node"], guest["type"]
    base = f"/nodes/{node}/{kind}/{vmid}"
    current = await api("GET", f"{base}/status/current")
    config = await api("GET", f"{base}/config")
    config.pop("digest", None)

    details = _guest_row({**guest, **{k: current.get(k) for k in ("cpu", "mem", "maxmem", "uptime") if k in current}})
    details["config"] = config
    if current.get("status") == "running":
        try:
            if kind == "lxc":
                interfaces = await api("GET", f"{base}/interfaces")
                details["interfaces"] = [
                    {k: i.get(k) for k in ("name", "hwaddr", "inet", "inet6") if i.get(k)}
                    for i in interfaces or [] if i.get("name") != "lo"
                ]
            elif str(config.get("agent", "0")).split(",")[0] not in ("0", "enabled=0"):
                agent = await api("GET", f"{base}/agent/network-get-interfaces")
                details["interfaces"] = [
                    {
                        "name": i.get("name"),
                        "hwaddr": i.get("hardware-address"),
                        "addresses": [a.get("ip-address") for a in i.get("ip-addresses", [])],
                    }
                    for i in (agent or {}).get("result", []) if i.get("name") not in ("lo", "Loopback Pseudo-Interface 1")
                ]
        except ProxmoxError as exc:
            details["interfaces_error"] = str(exc)
    return _out(details)


@read_tool
async def node_status(node: str) -> str:
    """Hardware and OS status of one node: CPU model and load, memory, swap, root filesystem,
    kernel and Proxmox version, uptime."""
    s = await api("GET", f"/nodes/{node}/status")
    cpu = s.get("cpuinfo", {})
    return _out({
        "node": node,
        "pve_version": s.get("pveversion"),
        "kernel": (s.get("current-kernel") or {}).get("release") or s.get("kversion"),
        "uptime": _uptime(s.get("uptime")),
        "cpu": {
            "model": cpu.get("model"),
            "sockets": cpu.get("sockets"),
            "cores": cpu.get("cores"),
            "threads": cpu.get("cpus"),
            "usage_pct": _pct(s.get("cpu")),
            "loadavg": s.get("loadavg"),
            "iowait_pct": _pct(s.get("wait")),
        },
        "mem_gb": [_gb((s.get("memory") or {}).get("used")), _gb((s.get("memory") or {}).get("total"))],
        "swap_gb": [_gb((s.get("swap") or {}).get("used")), _gb((s.get("swap") or {}).get("total"))],
        "rootfs_gb": [_gb((s.get("rootfs") or {}).get("used")), _gb((s.get("rootfs") or {}).get("total"))],
    })


@read_tool
async def list_storage(node: str | None = None) -> str:
    """Storages with type, allowed content and usage ([used, total] in GB), optionally for one node.
    Use it to pick a storage for container disks (content includes "rootdir") or templates ("vztmpl")."""
    rows = []
    for r in await api("GET", "/cluster/resources", {"type": "storage"}):
        if node and r.get("node") != node:
            continue
        rows.append({
            "storage": r.get("storage"),
            "node": r.get("node"),
            "type": r.get("plugintype"),
            "status": r.get("status"),
            "content": r.get("content"),
            "shared": bool(r.get("shared")),
            "used_gb": [_gb(r.get("disk")), _gb(r.get("maxdisk"))],
        })
    return _out(sorted(rows, key=lambda s: (str(s["node"]), str(s["storage"]))))


@read_tool
async def list_templates(node: str, available: bool = False, search: str | None = None) -> str:
    """Container templates on a node.

    available=False: templates already downloaded (their `volid` is what create_container needs).
    available=True: templates that can be downloaded with download_template.
    search: case-insensitive substring filter, e.g. "debian-13" or "ubuntu"."""
    needle = (search or "").lower()
    if available:
        rows = [
            {k: t.get(k) for k in ("template", "os", "version", "section", "headline")}
            for t in await api("GET", f"/nodes/{node}/aplinfo")
            if needle in str(t.get("template", "")).lower()
        ]
        return _out(sorted(rows, key=lambda t: str(t["template"]))[:60])

    rows = []
    for storage in await api("GET", f"/nodes/{node}/storage", {"content": "vztmpl"}):
        if not storage.get("active", 1):
            continue
        name = storage["storage"]
        for item in await api("GET", f"/nodes/{node}/storage/{name}/content", {"content": "vztmpl"}):
            if needle in str(item.get("volid", "")).lower():
                rows.append({"volid": item.get("volid"), "storage": name, "size_gb": _gb(item.get("size"))})
    return _out(rows)


@read_tool
async def list_isos(node: str, search: str | None = None) -> str:
    """ISO images on a node (installers, driver discs such as virtio-win). Their `volid` is what
    create_vm takes in `cdroms`. Each node has its own `local` storage, so an ISO on one node is
    not visible on another. search: case-insensitive substring filter."""
    needle = (search or "").lower()
    rows = []
    for storage in await api("GET", f"/nodes/{node}/storage", {"content": "iso"}):
        if not storage.get("active", 1):
            continue
        name = storage["storage"]
        for item in await api("GET", f"/nodes/{node}/storage/{name}/content", {"content": "iso"}):
            if needle in str(item.get("volid", "")).lower():
                rows.append({"volid": item.get("volid"), "storage": name, "size_gb": _gb(item.get("size"))})
    return _out(rows)


@read_tool
async def list_tasks(node: str | None = None, errors_only: bool = False, limit: int = 20) -> str:
    """Recent Proxmox tasks across the cluster (backups, starts, creations...), newest first.
    errors_only: only finished tasks whose status is not OK."""
    rows = []
    for t in await api("GET", "/cluster/tasks"):
        if node and t.get("node") != node:
            continue
        finished = t.get("endtime") is not None
        if errors_only and (not finished or t.get("status") == "OK"):
            continue
        rows.append({
            "upid": t.get("upid"),
            "node": t.get("node"),
            "type": t.get("type"),
            "id": t.get("id"),
            "user": t.get("user"),
            "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t.get("starttime", 0))),
            "status": t.get("status") if finished else "running",
        })
    rows.sort(key=lambda t: t["started"], reverse=True)
    return _out(rows[: max(1, min(limit, 200))])


@read_tool
async def task_status(upid: str, log_lines: int = 30) -> str:
    """State of a task by UPID (running / finished, exit status) with the tail of its log."""
    node = _upid_node(upid)
    status = await api("GET", f"/nodes/{node}/tasks/{upid}/status")
    return _out({
        "upid": upid,
        "type": status.get("type"),
        "id": status.get("id"),
        "state": status.get("status"),
        "exitstatus": status.get("exitstatus"),
        "log": await _task_log_tail(upid, max(1, min(log_lines, 500))),
    })


@read_tool
async def list_snapshots(vmid: int) -> str:
    """Snapshots of a VM or container."""
    guest = await _locate(vmid)
    snapshots = await api("GET", f"/nodes/{guest['node']}/{guest['type']}/{vmid}/snapshot")
    return _out([
        {
            "name": s.get("name"),
            "description": s.get("description"),
            "created": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s["snaptime"])) if s.get("snaptime") else None,
            "parent": s.get("parent"),
        }
        for s in snapshots or [] if s.get("name") != "current"
    ])


# --------------------------------------------------------------------------- #
# Management tools (change state; not registered when PROXMOX_READ_ONLY=true)
# --------------------------------------------------------------------------- #

@write_tool()
async def guest_power(vmid: int, action: str) -> str:
    """Start or stop a VM or container.

    action: "start", "shutdown" (clean, from inside the guest), "reboot", or "stop" (hard power-off,
    may lose data - prefer shutdown)."""
    if action not in ("start", "shutdown", "reboot", "stop"):
        raise ProxmoxError('action must be "start", "shutdown", "reboot" or "stop"')
    guest = await _locate(vmid)
    result = await api("POST", f"/nodes/{guest['node']}/{guest['type']}/{vmid}/status/{action}")
    outcome = await _wait_task(result)
    return _out({"vmid": vmid, "name": guest.get("name"), "action": action, **outcome})


@write_tool()
async def wake_node(node: str) -> str:
    """Power on a switched-off cluster node with a Wake-on-LAN magic packet.

    The packet is broadcast by whichever online node answers the API, so at least one node
    must be running. The target node needs its MAC stored once: on any node run
    `pvesh set /nodes/<node>/config --wakeonlan <MAC>`. Check cluster_overview a minute later."""
    mac = await api("POST", f"/nodes/{node}/wakeonlan")
    return _out({"node": node, "magic_packet_for": mac, "sent_via": _preferred_host})


@write_tool()
async def create_container(
    node: str,
    hostname: str,
    ostemplate: str,
    storage: str,
    vmid: int | None = None,
    cores: int = 1,
    memory_mb: int = 512,
    swap_mb: int = 512,
    disk_gb: int = 8,
    bridge: str = "vmbr0",
    ip: str = "dhcp",
    gateway: str | None = None,
    nameserver: str | None = None,
    password: str | None = None,
    ssh_public_keys: str | None = None,
    unprivileged: bool = True,
    nesting: bool = True,
    onboot: bool = False,
    start: bool = True,
    tags: str | None = None,
    description: str | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    """Create an LXC container and wait for the task to finish.

    ostemplate: template volid from list_templates, e.g. "local:vztmpl/debian-13-standard_13.1-2_amd64.tar.zst".
    storage: storage for the root disk (see list_storage, content must include "rootdir").
    vmid: omit to take the next free ID.
    ip: "dhcp" or a static address in CIDR form such as "192.168.1.60/24" (then pass gateway).
    password / ssh_public_keys: root access to the container; give at least one or you can only
    enter it from the host with `pct enter`.
    nesting: needed for systemd in recent distros and for Docker inside the container.
    tags: semicolon-separated. extra: any further option of the Proxmox API call
    POST /nodes/{node}/lxc, e.g. {"mp0": "local-lvm:20,mp=/data", "net1": "..."}."""
    if ip != "dhcp" and "/" not in ip:
        raise ProxmoxError('ip must be "dhcp" or CIDR like "192.168.1.60/24"')
    if vmid is None:
        vmid = int(await api("GET", "/cluster/nextid"))

    net0 = f"name=eth0,bridge={bridge},ip={ip}"
    if gateway:
        net0 += f",gw={gateway}"
    params: dict[str, Any] = {
        "vmid": vmid,
        "hostname": hostname,
        "ostemplate": ostemplate,
        "cores": cores,
        "memory": memory_mb,
        "swap": swap_mb,
        "rootfs": f"{storage}:{disk_gb}",
        "net0": net0,
        "unprivileged": unprivileged,
        "features": "nesting=1" if nesting else None,
        "onboot": onboot,
        "start": start,
        "nameserver": nameserver,
        "password": password,
        "ssh-public-keys": ssh_public_keys,
        "tags": tags,
        "description": description,
    }
    params.update(extra or {})
    outcome = await _wait_task(await api("POST", f"/nodes/{node}/lxc", params), timeout=120)
    return _out({"vmid": vmid, "hostname": hostname, "node": node, **outcome})


@write_tool()
async def update_container_config(
    vmid: int,
    cores: int | None = None,
    memory_mb: int | None = None,
    swap_mb: int | None = None,
    hostname: str | None = None,
    onboot: bool | None = None,
    nameserver: str | None = None,
    tags: str | None = None,
    description: str | None = None,
    extra: dict[str, Any] | None = None,
    delete: str | None = None,
) -> str:
    """Change settings of an existing LXC container. Only the given options are touched.

    extra: any other option of PUT /nodes/{node}/lxc/{vmid}/config, e.g.
    {"net0": "name=eth0,bridge=vmbr0,ip=192.168.1.60/24,gw=192.168.1.1", "startup": "order=2",
    "features": "nesting=1,keyctl=1"}.
    delete: comma-separated option names to remove, e.g. "mp1,net1".
    CPU and memory apply live; some options need a container restart. Returns the new config.
    Bind mounts of host paths, device passthrough and raw lxc.* keys are root-only in Proxmox
    and cannot be set through an API token."""
    guest = await _locate_container(vmid)
    params: dict[str, Any] = {
        "cores": cores,
        "memory": memory_mb,
        "swap": swap_mb,
        "hostname": hostname,
        "onboot": onboot,
        "nameserver": nameserver,
        "tags": tags,
        "description": description,
        "delete": delete,
    }
    params.update(extra or {})
    if not any(v is not None for v in params.values()):
        raise ProxmoxError("Nothing to change: pass at least one option")
    base = f"/nodes/{guest['node']}/lxc/{vmid}"
    await api("PUT", f"{base}/config", params)
    config = await api("GET", f"{base}/config")
    config.pop("digest", None)
    return _out({"vmid": vmid, "config": config})


@write_tool()
async def resize_container_disk(vmid: int, size: str, disk: str = "rootfs") -> str:
    """Grow a container disk. size: "+4G" to add 4 GB, or an absolute size like "20G".
    Disks can only grow, never shrink. disk: "rootfs" or a mount point such as "mp0"."""
    guest = await _locate_container(vmid)
    result = await api("PUT", f"/nodes/{guest['node']}/lxc/{vmid}/resize", {"disk": disk, "size": size})
    return _out({"vmid": vmid, "disk": disk, "size": size, **await _wait_task(result)})


@write_tool()
async def update_vm_config(
    vmid: int,
    tags: str | None = None,
    description: str | None = None,
    name: str | None = None,
    cores: int | None = None,
    memory_mb: int | None = None,
    onboot: bool | None = None,
    extra: dict[str, Any] | None = None,
    delete: str | None = None,
) -> str:
    """Change settings of an existing QEMU virtual machine. Only the given options are touched.

    tags: semicolon-separated, replaces the whole tag list. description: the Notes field (Markdown).
    extra: any other option of PUT /nodes/{node}/qemu/{vmid}/config, e.g. {"startup": "order=3"}.
    delete: comma-separated option names to remove.
    Tags, description, name and onboot apply at once. On a running VM, CPU and memory changes
    stay pending until the VM is fully stopped and started again. Returns the new config.
    PCI passthrough (hostpci), hookscript and raw args are root-only in Proxmox and cannot
    be set through an API token."""
    guest = await _locate_vm(vmid)
    params: dict[str, Any] = {
        "tags": tags,
        "description": description,
        "name": name,
        "cores": cores,
        "memory": memory_mb,
        "onboot": onboot,
        "delete": delete,
    }
    params.update(extra or {})
    if not any(v is not None for v in params.values()):
        raise ProxmoxError("Nothing to change: pass at least one option")
    base = f"/nodes/{guest['node']}/qemu/{vmid}"
    await api("PUT", f"{base}/config", params)
    config = await api("GET", f"{base}/config")
    config.pop("digest", None)
    return _out({"vmid": vmid, "name": guest.get("name"), "config": config})


@write_tool()
async def create_vm(
    node: str,
    name: str,
    disk_storage: str,
    disk_gb: int,
    vmid: int | None = None,
    ostype: str = "l26",
    cores: int = 2,
    memory_mb: int = 2048,
    balloon: bool = True,
    cpu: str = "host",
    bios: str = "ovmf",
    machine: str = "q35",
    secure_boot_keys: bool = True,
    tpm: bool = False,
    disk_options: str = "iothread=1,discard=on,ssd=1",
    bridge: str = "vmbr0",
    cdroms: list[str] | None = None,
    usb: list[str] | None = None,
    boot_order: str | None = None,
    agent: bool = True,
    tablet: bool = True,
    onboot: bool = False,
    start: bool = False,
    tags: str | None = None,
    description: str | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    """Create a QEMU virtual machine and wait for the task to finish.

    ostype: "win11", "win10", "l26" (Linux) and so on. Windows 11 needs bios="ovmf" and tpm=True.
    disk_storage / disk_gb: the system disk scsi0 on a virtio-scsi-single controller (see list_storage).
    bios: "ovmf" (UEFI, adds an EFI disk on disk_storage) or "seabios".
    secure_boot_keys: with ovmf, pre-enroll the Microsoft keys (Secure Boot on).
    balloon=False: remove the balloon device. Memory is fixed either way unless a lower `balloon`
    target is set via extra; with PCI passthrough all guest RAM is pinned anyway. Keeping the
    device (default) lets Proxmox show the guest's real memory use.
    cdroms: ISO volids from list_isos, attached in order as ide2, ide0, ide1, ide3.
    usb: USB devices in order as usb0, usb1...: "host=046d:c52b" (vendor:product) or
    "mapping=<name>" (a cluster resource mapping). Proxmox lets only root configure raw host
    devices; an API token needs a resource mapping and the Mapping.Use privilege.
    boot_order: e.g. "usb0;scsi0" or "ide2;scsi0"; omitted = Proxmox default.
    extra: any further option of POST /nodes/{node}/qemu, e.g. {"vga": "std", "sockets": 1}.
    PCI passthrough (hostpci) and hookscripts are root-only in Proxmox."""
    if bios not in ("ovmf", "seabios"):
        raise ProxmoxError('bios must be "ovmf" or "seabios"')
    if vmid is None:
        vmid = int(await api("GET", "/cluster/nextid"))
    params: dict[str, Any] = {
        "vmid": vmid,
        "name": name,
        "ostype": ostype,
        "machine": machine,
        "bios": bios,
        "cpu": cpu,
        "sockets": 1,
        "cores": cores,
        "memory": memory_mb,
        "balloon": None if balloon else 0,
        "scsihw": "virtio-scsi-single",
        "scsi0": f"{disk_storage}:{disk_gb}" + (f",{disk_options}" if disk_options else ""),
        "net0": f"virtio,bridge={bridge}",
        "agent": "enabled=1" if agent else None,
        "tablet": tablet,
        "onboot": onboot,
        "boot": f"order={boot_order}" if boot_order else None,
        "tags": tags,
        "description": description,
    }
    if bios == "ovmf":
        params["efidisk0"] = f"{disk_storage}:1,efitype=4m" + (",pre-enrolled-keys=1" if secure_boot_keys else "")
    if tpm:
        params["tpmstate0"] = f"{disk_storage}:1,version=v2.0"
    for slot, volid in zip(("ide2", "ide0", "ide1", "ide3"), cdroms or []):
        params[slot] = f"{volid},media=cdrom"
    if cdroms and len(cdroms) > 4:
        raise ProxmoxError("At most 4 cdroms")
    for i, device in enumerate(usb or []):
        params[f"usb{i}"] = device
    params.update(extra or {})
    outcome = await _wait_task(await api("POST", f"/nodes/{node}/qemu", params), timeout=120)
    if outcome.get("ok") and start:
        outcome["start"] = await _wait_task(await api("POST", f"/nodes/{node}/qemu/{vmid}/status/start"))
    return _out({"vmid": vmid, "name": name, "node": node, **outcome})


@write_tool(destructive=True)
async def delete_vm(vmid: int, confirm_name: str) -> str:
    """Permanently delete a QEMU virtual machine and its disks. Irreversible.

    confirm_name must equal the VM's current name - a guard against a wrong VMID.
    The VM must be stopped first. It is also removed from backup jobs and HA."""
    guest = await _locate_vm(vmid)
    if guest.get("name") != confirm_name:
        raise ProxmoxError(
            f"Name mismatch: VM {vmid} is named {guest.get('name')!r}, not {confirm_name!r}. Nothing deleted."
        )
    if guest.get("status") == "running":
        raise ProxmoxError(f"VM {vmid} is running. Stop it first with guest_power.")
    result = await api(
        "DELETE", f"/nodes/{guest['node']}/qemu/{vmid}",
        {"purge": True, "destroy-unreferenced-disks": True},
    )
    return _out({"deleted": vmid, "name": confirm_name, **await _wait_task(result)})


@write_tool()
async def download_template(node: str, storage: str, template: str) -> str:
    """Download a container template from the Proxmox catalogue to a storage.
    template: a name from list_templates(available=True); storage must allow content "vztmpl"."""
    result = await api("POST", f"/nodes/{node}/aplinfo", {"storage": storage, "template": template})
    outcome = await _wait_task(result, timeout=120)
    return _out({"template": template, "volid": f"{storage}:vztmpl/{template}", **outcome})


@write_tool()
async def create_snapshot(vmid: int, name: str, description: str | None = None) -> str:
    """Snapshot a VM or container (needs snapshot-capable storage such as LVM-thin or ZFS).
    name: letters, digits, "_" and "-", starting with a letter."""
    guest = await _locate(vmid)
    result = await api(
        "POST", f"/nodes/{guest['node']}/{guest['type']}/{vmid}/snapshot",
        {"snapname": name, "description": description},
    )
    return _out({"vmid": vmid, "snapshot": name, **await _wait_task(result)})


@write_tool(destructive=True)
async def rollback_snapshot(vmid: int, name: str) -> str:
    """Roll a VM or container back to a snapshot. Everything changed since the snapshot is lost."""
    guest = await _locate(vmid)
    result = await api("POST", f"/nodes/{guest['node']}/{guest['type']}/{vmid}/snapshot/{name}/rollback")
    return _out({"vmid": vmid, "rolled_back_to": name, **await _wait_task(result, timeout=120)})


@write_tool(destructive=True)
async def delete_snapshot(vmid: int, name: str) -> str:
    """Delete one snapshot of a VM or container."""
    guest = await _locate(vmid)
    result = await api("DELETE", f"/nodes/{guest['node']}/{guest['type']}/{vmid}/snapshot/{name}")
    return _out({"vmid": vmid, "deleted_snapshot": name, **await _wait_task(result)})


@write_tool()
async def backup_guest(vmid: int, storage: str, mode: str = "snapshot", notes: str | None = None) -> str:
    """Start a vzdump backup of a VM or container to a backup storage (content "backup",
    e.g. a Proxmox Backup Server storage). mode: "snapshot" (no downtime), "suspend" or "stop".
    Backups run long: this returns the task UPID to follow with task_status."""
    guest = await _locate(vmid)
    result = await api(
        "POST", f"/nodes/{guest['node']}/vzdump",
        {"vmid": vmid, "storage": storage, "mode": mode, "notes-template": notes},
    )
    return _out({"vmid": vmid, "storage": storage, **await _wait_task(result, timeout=15)})


@write_tool(destructive=True)
async def delete_container(vmid: int, confirm_hostname: str) -> str:
    """Permanently delete an LXC container and its disks. Irreversible.

    confirm_hostname must equal the container's current hostname - a guard against a wrong VMID.
    The container must be stopped first. It is also removed from backup jobs and HA."""
    guest = await _locate_container(vmid)
    if guest.get("name") != confirm_hostname:
        raise ProxmoxError(
            f"Hostname mismatch: container {vmid} is named {guest.get('name')!r}, not {confirm_hostname!r}. Nothing deleted."
        )
    if guest.get("status") == "running":
        raise ProxmoxError(f"Container {vmid} is running. Stop it first with guest_power.")
    result = await api(
        "DELETE", f"/nodes/{guest['node']}/lxc/{vmid}",
        {"purge": True, "destroy-unreferenced-disks": True},
    )
    return _out({"deleted": vmid, "hostname": confirm_hostname, **await _wait_task(result)})


if SSH_HOSTS:

    @write_tool()
    async def container_exec(vmid: int, command: str, timeout_seconds: int = 120) -> str:
        """Run a shell command as root inside a running LXC container and return its output.
        Goes over SSH to the node (`pct exec`), so it works without network access to the container.
        Use for provisioning: apt install, writing config files, enabling services.
        Commands must be non-interactive (e.g. `DEBIAN_FRONTEND=noninteractive apt-get -y ...`)."""
        guest = await _locate_container(vmid)
        target = SSH_HOSTS.get(guest["node"])
        if not target:
            raise ProxmoxError(
                f"No SSH target for node {guest['node']!r}. Configured nodes: {', '.join(SSH_HOSTS)}"
            )
        remote = f"pct exec {int(vmid)} -- sh -lc {shlex.quote(command)}"
        targets = [t.strip() for t in target.split("|") if t.strip()]
        for index, current in enumerate(targets):
            last = index == len(targets) - 1
            if not last:
                # cheap reachability check so a dead LAN address fails over fast
                probe = await asyncio.create_subprocess_exec(
                    "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", current, "true",
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                if await probe.wait() == 255:
                    continue
            try:
                process = await asyncio.create_subprocess_exec(
                    "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", current, remote,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
            except FileNotFoundError:
                raise ProxmoxError("The `ssh` client was not found on this machine")
            try:
                raw, _ = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise ProxmoxError(f"Command did not finish within {timeout_seconds}s and was killed")
            break
        output = raw.decode("utf-8", errors="replace")
        if len(output) > MAX_EXEC_OUTPUT:
            output = "[... output truncated, showing the end ...]\n" + output[-MAX_EXEC_OUTPUT:]
        return _out({"vmid": vmid, "exit_code": process.returncode, "output": output})


if __name__ == "__main__":
    mcp.run()
