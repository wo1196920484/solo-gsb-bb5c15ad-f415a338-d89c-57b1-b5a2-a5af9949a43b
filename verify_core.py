"""Stdlib-only smoke test for the merge core (run without FastAPI installed)."""

import os
import tempfile
import threading
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

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL CORE CHECKS PASSED")
