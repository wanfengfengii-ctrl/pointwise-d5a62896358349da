#!/usr/bin/env python3
"""Contract verification for lake.Lake.

This is the single manifest verification entry point.  It exits non-zero if
any checked contract fails.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lake import Lake, LakeNotFound, LakeRejected  # noqa: E402

ROWS = [("A", 7), ("B", None), ("C", 11)]
TOTAL = 18


class LakeContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="lake-verify-")
        self.dir = Path(self._tmp.name)
        self.lake = Lake(self.dir)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def seal_demo(
        self,
        k: object = "k1",
        q_b: object = "b1",
        rows=ROWS,
        *,
        backup_before_seal: bool = True,
    ):
        b = self.lake.freeze(q_b, rows)
        if backup_before_seal:
            self.lake.backup(k)
        a = self.lake.admit(b, rows)
        r = self.lake.seal(a)
        if not backup_before_seal:
            self.lake.backup(k)
        g = self.lake.protect(k, r)
        return b, a, r, g

    def test_freeze_idempotent_and_conflict(self) -> None:
        self.assertEqual(self.lake.freeze("b", ROWS), "b")
        # Equal content, including tuple/list row forms, is the same artifact.
        self.assertEqual(
            self.lake.freeze("b", [["A", 7], ["B", None], ["C", 11]]), "b"
        )
        with self.assertRaises(LakeRejected):
            self.lake.freeze("b", [("A", 8), ("B", None), ("C", 11)])
        with self.assertRaises(LakeRejected):
            self.lake.freeze("b", [("A", 7), ("C", 11)])

        # A different number may freeze its own artifact.
        self.assertEqual(self.lake.freeze("b2", ROWS), "b2")

        # Persisted across reopening.
        reopened = Lake(self.dir)
        self.assertEqual(reopened.freeze("b", ROWS), "b")
        with self.assertRaises(LakeRejected):
            reopened.freeze("b", [("A", 7), ("B", 0), ("C", 11)])

    def test_freeze_rejects_bad_liters(self) -> None:
        with self.assertRaises(LakeRejected):
            self.lake.freeze("bad", [("A", True)])  # bool is not an integer liter
        with self.assertRaises(LakeRejected):
            self.lake.freeze("bad", [("A", 7.5)])  # type: ignore[list-item]
        with self.assertRaises(LakeRejected):
            self.lake.freeze("bad", [("A", "7")])  # type: ignore[list-item]

    def test_admit_requires_line_by_line_match(self) -> None:
        self.lake.freeze("b", ROWS)
        with self.assertRaises(LakeRejected):
            self.lake.admit("b", [("A", 7), ("C", 11), ("B", None)])
        with self.assertRaises(LakeRejected):
            self.lake.admit("b", [("A", 7), ("B", None)])

        a = self.lake.admit("b", ROWS)
        self.assertTrue(isinstance(a, str) and a)
        self.assertEqual(self.lake.admit("b", ROWS), a)

        with self.assertRaises(LakeNotFound):
            self.lake.admit("never-frozen", ROWS)

    def test_seal_read_original_order_and_sum(self) -> None:
        b = self.lake.freeze("b", ROWS)
        a = self.lake.admit(b, ROWS)
        r = self.lake.seal(a)
        result_r, rows, total = self.lake.read(r)
        self.assertEqual(result_r, r)
        self.assertEqual(rows, ROWS)
        self.assertEqual(total, TOTAL)

        # Reopening reads the same hot ledger.
        result = Lake(self.dir).read(r)
        self.assertEqual(result, (r, ROWS, TOTAL))

    def test_seal_empty_whole_item_is_distinguishable_from_missing(self) -> None:
        rows: list[tuple[str, None]] = []
        b = self.lake.freeze("empty", rows)
        a = self.lake.admit(b, rows)
        r = self.lake.seal(a)
        self.assertEqual(self.lake.read(r), (r, [], 0))

    def test_restore_from_backup_taken_after_seal(self) -> None:
        b = self.lake.freeze("b-after", ROWS)
        a = self.lake.admit(b, ROWS)
        r = self.lake.seal(a)
        k = "after-seal"
        self.lake.backup(k)
        g = self.lake.protect(k, r)

        # Restore is idempotent when the backup already contains r.
        self.lake.restore(k, g, r)
        self.assertEqual(self.lake.read(r), (r, ROWS, TOTAL))

    def test_backup_before_seal_restore_with_hook_purge_falls_back(self) -> None:
        b, a, r, g = self.seal_demo("pre")
        self.lake.rollback_hot("pre")
        with self.assertRaises(LakeNotFound):
            self.lake.read(r)

        calls: list[str] = []
        state: dict[str, object] = {}

        def hook(v: str) -> None:
            calls.append(v)
            self.assertEqual(len(calls), 1)
            state["first"] = v

            other = Lake(self.dir)
            permit, deleted = other.permit("q1", r, v)
            self.assertFalse(deleted)

            # Same q/params reopens the original permit without double spend.
            again, still_not_deleted = Lake(self.dir).permit("q1", r, v)
            self.assertEqual(again, permit)
            self.assertFalse(still_not_deleted)

            # A different permit identity cannot spend the same volume again,
            # even though purge has not happened yet.
            with self.assertRaises(LakeRejected):
                Lake(self.dir).permit("q-other", r, v)

            # The restore result is not published inside the callback.
            with self.assertRaises(LakeNotFound):
                other.read(r)

            other.purge(permit)
            self.assertFalse(Path(v).exists())

            reopened, deleted_now = Lake(self.dir).permit("q1", r, v)
            self.assertEqual(reopened, permit)
            self.assertTrue(deleted_now)

        self.lake.restore("pre", g, r, hook)
        self.assertEqual(len(calls), 1)

        first = Path(str(state["first"]))
        self.assertFalse(first.exists())
        self.assertTrue((self.dir / "C").exists())

        got_r, rows, total = self.lake.read(r)
        self.assertEqual((got_r, rows, total), (r, ROWS, TOTAL))

    def test_hook_permit_without_purge_still_spends_selection(self) -> None:
        _, _, r, g = self.seal_demo("k-spend")
        self.lake.rollback_hot("k-spend")

        def hook(v: str) -> None:
            permit, deleted = Lake(self.dir).permit("q-spend", r, v)
            self.assertFalse(deleted)
            # Deliberately do not purge v.  Issuance itself spends it.
            self.assertTrue(Path(v).exists())
            state["permit"] = permit

        state: dict[str, object] = {}
        self.lake.restore("k-spend", g, r, hook)
        self.assertEqual(self.lake.read(r)[2], TOTAL)

        # The issued permit still reports the physical volume as present.
        _, deleted = Lake(self.dir).permit("q-spend", r, self.dir / "S")
        self.assertFalse(deleted)

    def test_permit_same_q_different_parameters_rejected(self) -> None:
        _, _, r, _ = self.seal_demo("k-q")
        p, deleted = self.lake.permit("q", r, "S")
        self.assertFalse(deleted)
        self.assertEqual(self.lake.permit("q", r, str(self.dir / "S"))[0], p)

        with self.assertRaises(LakeRejected):
            self.lake.permit("q", r, "C")

        other_rows = [("Z", 1)]
        b2 = self.lake.freeze("b2", other_rows)
        a2 = self.lake.admit(b2, other_rows)
        r2 = self.lake.seal(a2)
        with self.assertRaises(LakeRejected):
            self.lake.permit("q", r2, "S")

        self.lake.purge(p)
        p2, deleted2 = Lake(self.dir).permit("q", r, "S")
        self.assertEqual(p2, p)
        self.assertTrue(deleted2)

    def test_no_qualified_volume_rejects_entire_restore(self) -> None:
        _, _, r, g = self.seal_demo("k-none")
        ps, _ = self.lake.permit("qs", r, "S")
        pc, _ = self.lake.permit("qc", r, "C")
        self.lake.purge(ps)
        self.lake.purge(pc)
        self.lake.rollback_hot("k-none")

        with self.assertRaises(LakeRejected):
            self.lake.restore("k-none", g, r)

        # Entire operation rejected: r is still absent from hot ledger.
        hot = self.lake._hot_connect()
        try:
            self.assertIsNone(self.lake._read_hot_rows(hot, r))
        finally:
            hot.close()
        # Both volumes are gone, so the public read is due/expired.
        self.assertEqual(self.lake.read(r), (r, "已到期"))

    def test_issued_permits_without_purge_still_invalidate_rp(self) -> None:
        _, _, r, g = self.seal_demo("k-spent-both")
        _, deleted_s = self.lake.permit("qs-spent", r, "S")
        _, deleted_c = Lake(self.dir).permit("qc-spent", r, "C")
        self.assertFalse(deleted_s)
        self.assertFalse(deleted_c)

        # Both physical volumes are still present, but both qualifications are
        # spent merely by issuing permits.
        self.assertTrue((self.dir / "S").exists())
        self.assertTrue((self.dir / "C").exists())
        self.lake.rollback_hot("k-spent-both")
        with self.assertRaises(LakeRejected):
            self.lake.restore("k-spent-both", g, r)

    def test_invalid_bindings_reject_restore(self) -> None:
        _, _, r, g = self.seal_demo("k-good")
        self.lake.backup("k-other")
        self.lake.rollback_hot("k-good")

        with self.assertRaises(LakeRejected):
            self.lake.restore("k-other", g, r)
        with self.assertRaises(LakeRejected):
            self.lake.restore("k-good", g, "r-does-not-exist")
        with self.assertRaises((LakeNotFound, LakeRejected)):
            self.lake.restore("k-good", "g-bogus", r)

        with self.assertRaises(LakeNotFound):
            self.lake.read(r)

    def test_backup_tampering_rejected(self) -> None:
        _, _, r, g = self.seal_demo("k-tamper")
        backup = self.lake.backup_path("k-tamper")
        with backup.open("ab") as handle:
            handle.write(b"tampered-evidence")
        self.lake.rollback_hot(None)

        with self.assertRaises(LakeRejected):
            self.lake.protect("k-tamper", r)
        with self.assertRaises(LakeRejected):
            self.lake.restore("k-tamper", g, r)

    def test_volume_mismatch_insufficient_evidence_rejects(self) -> None:
        _, _, r, g = self.seal_demo("k-mismatch")
        receipt_hash = r[2:]  # receipt is r-<first hash chars>
        for name in ("S", "C"):
            for body in (self.dir / name).glob("*.json"):
                data = json.loads(body.read_text(encoding="utf-8"))
                data["rows"][0][1] = 999
                body.write_text(json.dumps(data), encoding="utf-8")

        self.lake.rollback_hot("k-mismatch")
        with self.assertRaises(LakeRejected):
            self.lake.restore("k-mismatch", g, r)

    def test_release_and_both_purges_expire_receipt(self) -> None:
        _, _, r, g = self.seal_demo("k-expire")

        ps, _ = self.lake.permit("qs", r, "S")
        pc, _ = self.lake.permit("qc", r, "C")
        self.lake.purge(ps)
        self.lake.purge(pc)

        # Before release, the release condition has not been satisfied.
        self.assertEqual(self.lake.read(r)[0], r)
        self.lake.release(g)

        result = Lake(self.dir).read(r)
        self.assertEqual(result, (r, "已到期"))

        # Releasing twice is harmless.
        self.lake.release(g)
        self.assertEqual(Lake(self.dir).read(r), (r, "已到期"))

    def test_release_before_purges_also_expires_after_second(self) -> None:
        _, _, r, g = self.seal_demo("k-expire2")
        self.lake.release(g)

        ps, _ = self.lake.permit("qs2", r, "S")
        self.lake.purge(ps)
        self.assertEqual(len(self.lake.read(r)), 3)

        pc, _ = self.lake.permit("qc2", r, "C")
        self.lake.purge(pc)
        self.assertEqual(self.lake.read(r), (r, "已到期"))

    def test_catalog_does_not_store_full_body(self) -> None:
        marker = ("独特出处_XYZ", 424242)
        rows = [marker, ("B", None)]
        _, _, r, _ = self.seal_demo("k-body", "b-body", rows=rows)

        catalog_bytes = (self.dir / "catalog.sqlite3").read_bytes()
        self.assertNotIn("独特出处_XYZ".encode(), catalog_bytes)
        self.assertNotIn(b"424242", catalog_bytes)

        # The complete long-term body exists under exactly S and C.
        for volume in ("S", "C"):
            bodies = list((self.dir / volume).glob("*.json"))
            self.assertEqual(len(bodies), 1)
            self.assertIn("独特出处_XYZ", bodies[0].read_text(encoding="utf-8"))

    def test_demo_entrypoint(self) -> None:
        root = Path(__file__).resolve().parent
        env = dict(os.environ, PYTHONPATH=str(root))
        result = subprocess.run(
            [sys.executable, "-B", str(root / "demo.py")],
            cwd=root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
        if result.returncode != 0:
            self.fail(f"demo failed:\nSTDOUT:{result.stdout}\nSTDERR:{result.stderr}")
        self.assertIn("18", result.stdout)
        self.assertIn("按原号", result.stdout)


def main() -> int:
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
