# OTW mode

OTW mode runs the SealMesh router on one of your own machines and
lets your other machines reach it over [Tailscale](https://tailscale.com). There
are no tokens. Tailscale already knows who's on the other end of each
connection, so the router asks it.

Pick it when your machines are all yours (a laptop, a desktop, a build box) and
you don't want GitHub or a cloud host in the loop.

## How it works

- The router listens **only on its machine's tailnet address**. Nothing off the
  tailnet can reach it.
- When a daemon connects, the router asks the local `tailscaled` who the peer
  is (LocalAPI `whois`). The answer gives it two things:
  - **who** — the Tailscale user's login name (`alice@github`), which becomes
    the host (`alice-github`). A node owned by an ACL tag has no user, so its
    host is the first allowed tag (`tag:sealmesh` becomes `tag-sealmesh`).
  - **which machine** — the Tailscale node name (`work-laptop`). Two of your
    machines are two rows in `sealmesh who`, and both stay connected.
- The router admits a peer only if its user is in `allow_users` or the node
  has a tag in `allow_tags`. That allowlist sits on top of your tailnet ACL.
- Everything an admitted identity asks to read is readable. OTW assumes a
  tailnet of one owner's machines.
- The connection is plain WebSocket. WireGuard already encrypts every hop
  inside the tailnet, and there's no credential in the handshake to leak.

## Prerequisites

- Tailscale on every machine, all on one tailnet.
- On the router's machine, the open-source `tailscaled`, with its LocalAPI
  socket readable by the user that runs the router:
  - Linux: `/var/run/tailscale/tailscaled.sock` (the default).
  - macOS: `/var/run/tailscaled.socket`, for `tailscaled` installed from
    Homebrew or source. The Mac App Store and standalone apps serve LocalAPI
    over a private TCP port instead of a socket, which the router doesn't
    speak. Run the router on Linux, or on a Mac running the open-source
    `tailscaled`.
- `sealmesh-server` on the router's machine, and `sealmesh` on every machine.

## Set up the router

Find the machine's tailnet address:

```sh
tailscale ip -4      # e.g. 100.101.102.103
```

Write `/etc/sealmesh/sealmesh-server.toml`:

```toml
mode = "otw"
listen = "100.101.102.103:8787"         # this machine's tailnet IP, nothing else

[otw]
allow_users = ["alice@github"]          # Tailscale login names
allow_tags = ["tag:sealmesh"]           # ACL tags, for tagged machines
# tailscaled_socket = "/var/run/tailscaled.socket"   # macOS, or a non-default path

[history]
path = "/var/lib/sealmesh/history.sqlite"

# Optional: named rooms. Members are hosts as the router names them.
# [[room]]
# name = "ops"
# members = ["alice-github", "tag-sealmesh"]
```

`allow_users` and `allow_tags` can't both be empty: a router nobody can join
refuses to start. `projects` works as in cloud mode and is optional here.
`github_org` isn't read.

The router refuses to start when:

- `listen` is `0.0.0.0`, `::`, loopback, a LAN address or a public one. It must
  be in `100.64.0.0/10` or `fd7a:115c:a1e0::/48`.
- `tls_terminated_by` is set. A proxy in front would hide each peer's tailnet
  address, and the router couldn't ask who it is.

Start it with `sealmesh-server --config /etc/sealmesh/sealmesh-server.toml`.

### systemd (Linux)

```ini
# /etc/systemd/system/sealmesh-server.service
[Unit]
Description=SealMesh router (otw)
After=tailscaled.service network-online.target
Wants=tailscaled.service network-online.target

[Service]
ExecStart=/usr/local/bin/sealmesh-server --config /etc/sealmesh/sealmesh-server.toml
Restart=on-failure
RestartSec=5
DynamicUser=yes
StateDirectory=sealmesh

[Install]
WantedBy=multi-user.target
```

`Restart=on-failure` covers the boot race where the tailnet address isn't up
yet and the bind fails. With `DynamicUser`, point `[history] path` at
`/var/lib/sealmesh/`.

### launchd (macOS, open-source tailscaled)

```xml
<!-- /Library/LaunchDaemons/dev.sealmesh.server.plist -->
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>dev.sealmesh.server</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/sealmesh-server</string>
    <string>--config</string>
    <string>/usr/local/etc/sealmesh/sealmesh-server.toml</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>/usr/local/var/log/sealmesh-server.log</string>
</dict>
</plist>
```

Load it with `sudo launchctl bootstrap system /Library/LaunchDaemons/dev.sealmesh.server.plist`.

## Restrict the port in your tailnet policy

The router's allowlist is the second gate. Make the tailnet ACL the first, so
only your machines can open the port at all. Tag the router's machine (say
`tag:sealmesh-router`) and grant just port 8787:

```jsonc
{
  "tagOwners": {
    "tag:sealmesh-router": ["autogroup:admin"],
    "tag:sealmesh":        ["autogroup:admin"]
  },
  "grants": [
    {
      "src": ["alice@github", "tag:sealmesh"],
      "dst": ["tag:sealmesh-router"],
      "ip":  ["tcp:8787"]
    }
  ]
}
```

With the older `acls` syntax, the same rule is
`{"action": "accept", "src": ["alice@github", "tag:sealmesh"], "dst": ["tag:sealmesh-router:8787"]}`.

## Set up each daemon

```toml
[mesh]
mode = "otw"

[server]
url = "http://100.101.102.103:8787"     # the router's tailnet IP

[allow]
repos = ["you/your-repo"]
paths = ["/Users/you/src"]
```

`[server] url` must name a tailnet IP or a MagicDNS name (`router`, or
`router.your-tailnet.ts.net`). A MagicDNS name works but `sealmesh doctor`
warns about it: it resolves only while MagicDNS is on, so the IP is the firmer
pin. A LAN, loopback or public IP is refused at load. Use `http://`: the
router serves plain WebSocket inside WireGuard, and a TLS proxy in front of it
would hide who each peer is.

The daemon sends no credential. `machine` in the config is ignored in otw mode:
the router names each machine from its Tailscale node name.

`sealmesh doctor` shows the mode, the router URL, and whether this machine's
`tailscaled` LocalAPI answers. Only the router's machine needs that; the others
just need to be on the tailnet.

## When something fails

The router fails closed. It refuses the connection (`Denied`) when:

- `tailscaled` is down, or its socket can't be opened;
- `whois` errors, answers anything but 200, or takes longer than 2 seconds;
- the reply can't be parsed, or has no node or no login;
- the peer isn't a tailnet address, or isn't in `allow_users` / `allow_tags`.

Each refused peer is logged once a minute at most, with the reason. Daemons
already back off and retry, so a `tailscaled` restart costs a retry, not an
open door.

A `whois` answer is cached per peer address for 60 seconds. A machine removed
from the tailnet, or a tag taken off, stops being admitted within a minute.
Failures aren't cached; the next connection asks again.

## Security model

- **Reachability:** the router binds only a tailnet address, and the tailnet
  ACL decides which nodes can open its port.
- **Encryption:** WireGuard, end to end between nodes. No TLS needed, and no
  token is sent to protect.
- **Identity:** `tailscaled`'s `whois` answer for the connection's own source
  address. A daemon can't claim to be someone else, and the router names its
  machine — the daemon's own `machine` claim is ignored.
- **Authorization:** the router's `allow_users` / `allow_tags`, checked on
  every connection (cached up to 60 s).
- **Access:** an admitted identity can read everything the router serves. OTW
  is for one owner's machines. Share the tailnet with others and they're
  bounded only by the allowlist, so don't list them.
- **Trust in the local machine:** anyone who can write to the router machine's
  `tailscaled` socket, or edit its config, controls who gets in. That's the
  same root of trust as Tailscale itself on that machine.

## What it relies on from Tailscale

`GET /localapi/v0/whois?addr=<ip:port>` with `Host: local-tailscaled.sock`, and
these fields of its reply (Go field names, from `apitype.WhoIsResponse`,
`tailcfg.Node` and `tailcfg.UserProfile` in
[tailscale/tailscale](https://github.com/tailscale/tailscale)):

| Field | Used for |
|---|---|
| `Node.ComputedName` | the machine name (falls back to `Node.Name`'s first label) |
| `Node.Name` | the node's FQDN, as that fallback |
| `Node.Tags` | a tagged node's identity and `allow_tags` |
| `UserProfile.LoginName` | the person, and `allow_users` |
