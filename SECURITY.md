# Security policy

## Supported versions

Only the latest commit on `main` (and the latest release, once there is one) receives fixes.

## Reporting a vulnerability

Please **do not open a public issue**. Report it privately through GitHub
([Security advisories](https://github.com/cr0hn/async-sonic/security/advisories/new)) or by email to
cr0hn@cr0hn.com with a description, steps to reproduce and the affected version or commit.

You can expect an acknowledgement within a few days. Please allow reasonable time for a fix before
disclosing the issue publicly.

## Scope notes

- `async-sonic` speaks Sonic Channel, which is **plain TCP without TLS**. Do not expose a Sonic
  server to untrusted networks, and treat the channel password as sensitive.
- The client validates identifiers (collection, bucket, object) and escapes text so that user input
  cannot inject extra protocol commands. A way around that validation is a vulnerability: report it.
