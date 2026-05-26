---
"@farcaster/shuttle": patch
---

Scrub Postgres-uncastable characters (NUL and unpaired UTF-16 surrogates) from message body JSON before insert. These pass `json` validation but throw on `body::jsonb` and on any `body->>'key'` extraction, leaving rows that look fine at INSERT and blow up at read time.
