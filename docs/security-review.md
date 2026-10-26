# Security and bug review: swpt_stomp

| | |
|---|---|
| Reviewed commit | `3253177` (2026-10-14) |
| Review date | 2026-10-26 |
| Scope | `swpt_stomp/` (STOMP parser, server and client TLS setup, peer authentication, message validation and routing, flow control), `swpt-certificates.rst` |
| Method | Manual code review; parser findings reproduced with the parser directly; the test suite run without a RabbitMQ broker |

This document covers the issue **fixed in this pull request**, plus
review notes.

## Summary

| ID | Severity | Finding | Status |
|---|---|---|---|
| BUG-1 | Low | A negative `content-length` makes the parser split one frame into two | Fixed |

## BUG-1: Negative content-length causes frame-boundary confusion (Low, fixed)

**Where:** `swpt_stomp/stomp_parser.py`, `StompParser._parse_command`.

**Problem.** `content-length` was parsed with `int()` and checked only
against the upper limit. A negative value such as `-4` set the body's
end *before* the end of the headers. The parser then searched for the
frame-terminating NULL starting inside the header block. Header values
may contain NULL bytes, so the first frame ended early, and the real
body was parsed as a **second, separate frame**.

```
SEND\ncontent-length:-4\nx:\0\n\nSEND\ndestination:/exchange/smp\n\nsmuggled\0

→ frame 1: SEND {'content-length': '-4', 'x': '\x00'} body=b''
→ frame 2: SEND {'destination': '/exchange/smp'} body=b'smuggled'
```

Both frames come from the same authenticated peer, which could just
send two frames, so the security impact is low. It is still a parser
correctness bug, and it would matter if any other component parsed the
same byte stream differently.

**Fix.** A negative `content-length` now raises
`ProtocolError("Content-length is negative.")`.

**Test:** `test_content_length_negative` (3 cases).

## Review notes

- **Peer authentication.** The server requires a client certificate
  (`CERT_REQUIRED`, TLS 1.3 only) chaining to the node's own root
  certificate. The peer is identified by the subject's `serialNumber`,
  and the `organizationName` must be `Swaptacular Nodes Registry`.
  Multi-valued or duplicated attributes are rejected; the first
  occurrence wins. Peer certificates carry a critical Name Constraints
  extension (per `swpt-certificates.rst`), so a peer cannot issue leaf
  certificates with another node's serial number. The client verifies
  that the server's serial number matches the expected peer.
- **Message authorization.** `preprocess_message` checks that
  `creditor_id` and `debtor_id` belong to the peer's subnets. It also
  checks that the coordinator type fits the node types (`direct`/`agent`
  for creditors agents, `issuing` for debtors agents), and that
  `coordinator_id` is consistent (`direct` → equals `creditor_id`,
  `agent` → in the peer's creditors subnet, `issuing` → equals
  `debtor_id` with root creditor). Timestamps more than two hours in the
  future are rejected.
- **Resource limits.** Headers are limited to 4 KiB and 50 lines, bodies
  to 8 KiB; the receive queue applies backpressure (`pause_reading`);
  heartbeat intervals cannot go below the configured minimum; and the
  number of connections per peer is limited.
- **Peer database paths.** The file-based peer database joins the
  certificate's serial number into a path without validation. This is
  safe only because the serial number comes from a certificate signed by
  the node's own root (with Name Constraints). Validating it against the
  expected format would be cheap extra hardening.

## Tests

- `test_content_length_negative` (3 cases) fails without the fix.

Full suite: 105 passed, 1 skipped, 11 failed. The 11 failures
(`test_rmq.py`, `test_drainer.py`, `test_cli.py`,
`test_client_and_server.py`) need a running RabbitMQ broker, and fail
identically without this change. The two existing `flake8` warnings
(`drainer.py:34 F401`, `stomp_parser.py:238 E203`) predate this change.
`mypy` is clean.
