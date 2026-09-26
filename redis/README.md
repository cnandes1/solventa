# Redis Business ACL (AS-8)

`users.acl` is loaded by `redis-business` with `--aclfile`. Passwords are stored as
SHA-256 hashes; the plaintext TEST ONLY values live only in `docker-compose.yaml`.

| User | Used by | Permission on `profile-updated` |
|---|---|---|
| `default` (no password) | Docker healthcheck, anonymous clients | none (`PING` only) |
| `profile_producer` | Profiling | `XADD` only |
| `profile_consumer` | Quoting A/B | `XGROUP CREATE`, `XREADGROUP`, `XACK`, `XAUTOCLAIM` only |

Each user keeps the minimum it needs on its other working keys (Profiling: cache,
versions and voting streams; Quoting: `XADD` to `profile-refresh-requests`).
Regenerate a hash with `printf %s "<password>" | shasum -a 256`.
