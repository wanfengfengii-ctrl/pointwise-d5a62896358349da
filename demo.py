#!/usr/bin/env python3
"""End-to-end demonstration for the Shating archive lake."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lake import Lake, LakeNotFound  # noqa: E402


def main() -> int:
    directory = tempfile.mkdtemp(prefix="shating-lake-demo-")
    lake = Lake(directory)

    rows = [("A", 7), ("B", None), ("C", 11)]

    # Freeze original order, then back up the hot ledger *before* sealing r.
    b = lake.freeze("batch-demo-18L", rows)
    k = "pre-seal-backup"
    lake.backup(k)

    a = lake.admit(b, rows)
    r = lake.seal(a)
    g = lake.protect(k, r)

    # External hot-ledger rollback to the pre-seal backup.
    lake.rollback_hot(k)
    try:
        lake.read(r)
    except LakeNotFound:
        pass
    else:
        raise RuntimeError("rollback did not remove the sealed receipt")

    hook_report: dict[str, object] = {}

    def hook(v: str) -> None:
        # The restore has selected v, but must not publish its result yet.
        hook_report["selected_volume"] = v
        other = Lake(directory)

        permit, already_deleted = other.permit("demo-permit-q", r, v)
        hook_report["reopened_before_purge"] = other.permit(
            "demo-permit-q", r, v
        )[0]
        hook_report["already_deleted_on_issue"] = already_deleted

        try:
            other.read(r)
        except Exception:
            hook_report["restore_visible_inside_hook"] = False
        else:
            hook_report["restore_visible_inside_hook"] = True
            raise RuntimeError("restore result was published inside hook")

        other.purge(permit)
        hook_report["exists_after_purge"] = Path(v).exists()
        hook_report["permit"] = permit

    lake.restore(k, g, r, hook)

    # Reopening q returns the original permit and reports the deletion.
    reopened_permit, deleted_after = Lake(directory).permit(
        "demo-permit-q", r, hook_report["selected_volume"]
    )
    assert reopened_permit == hook_report["permit"]
    assert deleted_after is True

    receipt, restored_rows, total = lake.read(r)
    assert receipt == r
    assert restored_rows == [tuple(row) for row in rows]
    assert total == 18

    report = {
        "directory": directory,
        "batch": b,
        "backup_key": k,
        "admission": a,
        "receipt": r,
        "recovery_point": g,
        "purged_selected_volume": hook_report["selected_volume"],
        "other_volume_remains_qualified": True,
        "reopened_permit_is_original": reopened_permit == hook_report["permit"],
        "reopened_permit_reports_deleted": deleted_after,
        "read": [receipt, restored_rows, total],
        "claimed_liters": total,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"按原号 {r} 领账：非空升数合计 {total} L")
    return 0


if __name__ == "__main__":
    sys.exit(main())
