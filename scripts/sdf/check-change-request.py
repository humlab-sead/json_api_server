#!/usr/bin/env python3
"""
Read-only check of an SDF change-request bundle against a database.

Every script is run inside BEGIN READ ONLY ... ROLLBACK, which PostgreSQL itself
prevents from writing: guard and verify blocks (plain SELECTs and RAISE) run for
real, and every INSERT/UPDATE/DELETE/setval is only EXPLAINed (planned, never
executed). Expected outcome on the database the bundle was generated against:

  deploy  guards pass, every statement plans
  revert  guards fail (nothing is deployed yet), every statement plans
  verify  fails (nothing is deployed yet)

Usage: check-change-request.py <bundle dir> [psql command ...]
  e.g. check-change-request.py cr/20260930_DML_SDF_SITE_1 podman exec -i sead-postgresql-1 psql -U postgres -d sead_staging
"""
import re, subprocess, sys, glob, os

bundle, psql = sys.argv[1], sys.argv[2:]

def statements(sql):
    """The guard DO block and the data statements, in file order."""
    out, lines, i = [], sql.split("\n"), 0
    while i < len(lines):
        line = lines[i]
        if re.match(r"^do \$\w+\$", line):
            tag = line.split()[1]
            j = i
            while not lines[j].startswith(f"end {tag};"):
                j += 1
            out.append(("block", "\n".join(lines[i:j + 1])))
            i = j + 1
            continue
        if re.match(r"^(insert|update|delete|select setval)\b", line):
            j = i
            while not lines[j].rstrip().endswith(";"):
                j += 1
            out.append(("statement", "\n".join(lines[i:j + 1])))
            i = j + 1
            continue
        i += 1
    return out

def run(sql):
    p = subprocess.run(psql + ["-X", "-q", "-v", "ON_ERROR_STOP=1"], input=sql, capture_output=True, text=True)
    return p.returncode, (p.stderr or "").strip()

def check(kind, expect_guard_failure):
    path = glob.glob(os.path.join(bundle, kind, "*.sql"))[0]
    parts = statements(open(path, encoding="utf8").read())
    blocks = [p for k, p in parts if k == "block"]
    stmts = [p for k, p in parts if k == "statement"]
    ok = True
    for b in blocks:
        code, err = run("begin read only;\n" + b + "\nrollback;\n")
        failed = code != 0
        if failed != expect_guard_failure or (failed and "SDF" not in err):
            ok = False
        print(f"  {kind}: guard block {'raised' if failed else 'passed'}"
              f"{' (expected)' if failed == expect_guard_failure else ' (UNEXPECTED)'}"
              f"{': ' + err.splitlines()[0][:160] if failed else ''}")
    if stmts:
        code, err = run("begin read only;\n" + "\n".join("explain " + s for s in stmts) + "\nrollback;\n")
        print(f"  {kind}: {len(stmts)} statement(s) {'plan' if code == 0 else 'FAIL TO PLAN: ' + err[:300]}")
        ok = ok and code == 0
    return ok

results = [check("deploy", False), check("revert", True), check("verify", True)]
print("bundle OK" if all(results) else "bundle FAILED")
sys.exit(0 if all(results) else 1)
