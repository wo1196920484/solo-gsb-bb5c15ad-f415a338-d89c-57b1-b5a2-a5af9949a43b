"""Stdlib-only smoke test for the merge core (run without FastAPI installed)."""

import os
import tempfile
import threading
import time
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "verify.db")

from app import db, repo  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# 1. ordered accumulation
s, rep = repo.submit_event("s1", "e0", 0, 100)
check("first insert 201-ish replayed=False", not rep)
check("ordered balance 100", s.balance == 100 and s.next_sequence == 1 and s.pending_sequences == [], str(s))
s, rep = repo.submit_event("s1", "e1", 1, -30)
check("ordered balance 70", s.balance == 70 and s.next_sequence == 1 + 1)

# 2. gap buffered
s, _ = repo.submit_event("s2", "e1", 1, 200)
check("gap buffered", s.balance == 0 and s.next_sequence == 0 and s.pending_sequences == [1], str(s))
s, _ = repo.submit_event("s2", "e0", 0, 50)
check("catch-up applied both", s.balance == 250 and s.next_sequence == 2 and s.pending_sequences == [], str(s))

# 3. idempotent replay
s, rep = repo.submit_event("s1", "e0", 0, 100)
check("replay idempotent", rep and s.balance == 70 and s.next_sequence == 2, str(s))

# 4. replay conflict
try:
    repo.submit_event("s1", "e0", 1, 100)
    check("replay diff seq -> 409", False)
except repo.ConflictError as e:
    check("replay diff seq -> 409", e.code == "replay_conflict")
try:
    repo.submit_event("s1", "e0", 0, 999)
    check("replay diff delta -> 409", False)
except repo.ConflictError:
    check("replay diff delta -> 409", True)

# 5. duplicate sequence different event_id
try:
    repo.submit_event("s1", "other", 0, 100)
    check("dup sequence -> 409", False)
except repo.ConflictError as e:
    check("dup sequence -> 409", e.code == "sequence_conflict")

# 6. unknown stream
check("unknown stream None", repo.get_stream_state("nope") is None)

# 7. negative sequence rejected by DB CHECK
import sqlite3
try:
    repo.submit_event("s3", "ex", -1, 1)
    check("negative sequence rejected", False)
except sqlite3.IntegrityError:
    check("negative sequence rejected", True)

# 8. large out-of-order batch
for seq in range(1, 10):
    s, _ = repo.submit_event("s4", f"e{seq}", seq, seq)
check("batch pending", s.pending_sequences == list(range(1, 10)), str(s))
s, _ = repo.submit_event("s4", "e0", 0, 0)
check("batch catch-up", s.next_sequence == 10 and s.balance == sum(range(10)) and s.pending_sequences == [], str(s))

# 9. concurrency: 20 threads, scrambled order
errors = []
def post(seq):
    try:
        repo.submit_event("s5", f"e{seq}", seq, 1)
    except Exception:
        errors.append(traceback.format_exc())

threads = [threading.Thread(target=post, args=(seq,)) for seq in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent no errors", not errors, str(errors[:1]))
s = repo.get_stream_state("s5")
check("concurrent result", s.balance == 20 and s.next_sequence == 20 and s.pending_sequences == [], str(s))

# 10. persistence: reopen connection and re-read
s = repo.get_stream_state("s2")
check("persisted read", s.balance == 250 and s.next_sequence == 2)

# 11. consumer claim/ack cursor with leases
from app import consumers  # noqa: E402

os.environ["LEASE_TTL_SECONDS"] = "1"
for seq in range(6):
    repo.submit_event("s6", f"e{seq}", seq, 1)

c = consumers.claim("s6", "w1", 3)
check("claim first 3", c is not None and c.start_sequence == 0 and
      [e.sequence for e in c.events] == [0, 1, 2] and c.event_count == 3, str(c))
c_same = consumers.claim("s6", "w1", 100)
check("repeat claim identical lease", c_same.lease_token == c.lease_token and
      [e.sequence for e in c_same.events] == [0, 1, 2])

# other consumer is independent, wrong token rejected
other = consumers.claim("s6", "w2", 2)
try:
    consumers.ack("s6", "w1", other.lease_token)
    check("ack wrong token -> 409", False)
except repo.ConflictError as e:
    check("ack wrong token -> 409", e.code == "stale_lease_token")

a = consumers.ack("s6", "w1", c.lease_token)
check("ack advances cursor", a.cursor_sequence == 3, str(a))
try:
    consumers.ack("s6", "w1", c.lease_token)
    check("double ack -> 409", False)
except repo.ConflictError as e:
    check("double ack -> 409", e.code == "lease_not_found")

nxt = consumers.claim("s6", "w1", 10)
check("next claim continues at cursor", [e.sequence for e in nxt.events] == [3, 4, 5])
consumers.ack("s6", "w1", nxt.lease_token)
check("claim at tip is empty", consumers.claim("s6", "w1", 10) is None)

# expiry: same batch redelivered, old token rejected
time.sleep(1.1)
redelivered = consumers.claim("s6", "w2", 10)
check("expired lease redelivers from cursor",
      redelivered.lease_token != other.lease_token and
      [e.sequence for e in redelivered.events] == [0, 1, 2, 3, 4, 5])
try:
    consumers.ack("s6", "w2", other.lease_token)
    check("expired token ack -> 409", False)
except repo.ConflictError as e:
    check("expired token ack -> 409", e.code in ("lease_expired", "stale_lease_token"))

# 12. concurrent claims never produce parallel valid leases
for seq in range(20):
    repo.submit_event("s7", f"e{seq}", seq, 1)
lease_tokens = []
def claim():
    try:
        lease_tokens.append(consumers.claim("s7", "wg", 5).lease_token)
    except Exception:
        errors.append(traceback.format_exc())

threads = [threading.Thread(target=claim) for _ in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent claims one lease", len(set(lease_tokens)) == 1 and len(lease_tokens) == 20,
      str(set(lease_tokens)))

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL CORE CHECKS PASSED")
