"""Persistent archive ledger implemented with Python + SQLite.

The public entry point is :class:`Lake`.  The hot ledger is a replaceable
SQLite database; the catalog and the two long-term volumes (``S`` and ``C``)
live outside that rollback boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

__all__ = [
    "Lake",
    "LakeError",
    "LakeRejected",
    "LakeNotFound",
    "ConflictError",
    "VerificationError",
    "EvidenceError",
    "VolumeHandle",
]

SCHEMA_VERSION = 1
VOLUME_NAMES = ("S", "C")


class LakeError(Exception):
    """Base class for Lake failures."""


class LakeRejected(LakeError, ValueError):
    """The requested operation conflicts with evidence or is invalid."""


class LakeNotFound(LakeRejected, KeyError):
    """A receipt, key, recovery point, or permit does not exist."""

    def __str__(self) -> str:  # pragma: no cover - trivial
        args = self.args
        if len(args) == 1:
            return f"not found: {args[0]!r}"
        return super().__str__()


# Compatibility aliases for callers/tests using generic contract names.
Error = LakeError
Rejected = LakeRejected
NotFound = LakeNotFound
ConflictError = LakeRejected
VerificationError = LakeRejected
EvidenceError = LakeRejected


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _json_key(value: Any) -> str:
    """Stable key for caller-supplied identifiers.

    The documented identifiers are normally strings, but accepting JSON-like
    values makes re-opening by equivalent parameters robust.
    """

    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    except (TypeError, ValueError):
        return "repr:" + repr(value)


def _normalize_rows(rows: Iterable[Any]) -> list[tuple[Any, int | None]]:
    if isinstance(rows, (str, bytes, bytearray)):
        raise LakeRejected("rows must be an iterable of (source, liters) pairs")

    normalized: list[tuple[Any, int | None]] = []
    try:
        iterator = iter(rows)
    except TypeError as exc:
        raise LakeRejected("rows must be an iterable") from exc

    for index, row in enumerate(iterator):
        if not isinstance(row, (tuple, list)) or len(row) != 2:
            raise LakeRejected(f"row {index} must be a (source, liters) pair")
        source, liters = row
        try:
            json.dumps(source, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise LakeRejected(f"row {index} has a non-JSON source") from exc

        if liters is not None:
            if isinstance(liters, bool) or not isinstance(liters, int):
                raise LakeRejected(
                    f"row {index} liters must be an integer or None"
                )
        normalized.append((source, liters))

    return normalized


def _canonical_rows(rows: Sequence[tuple[Any, int | None]]) -> str:
    plain = [[source, liters] for source, liters in rows]
    return json.dumps(plain, ensure_ascii=False, separators=(",", ":"))


def _rows_hash(rows: Sequence[tuple[Any, int | None]]) -> str:
    return hashlib.sha256(_canonical_rows(rows).encode("utf-8")).hexdigest()


def _receipt_for_hash(row_hash: str) -> str:
    # A deterministic, fixed-width receipt: the same original order always
    # yields the same receipt.
    return "r-" + row_hash[:32]


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Lake:
    """An archive catalog rooted at *directory*.

    Parameters
    ----------
    directory:
        Persistent catalog directory.  It contains the rollback-able hot
        ledger, backup images, metadata catalog, and the two long-term
        volumes ``S`` and ``C``.
    """

    SCHEMA_VERSION = 1
    VOLUME_NAMES = ("S", "C")
    S = "S"
    C = "C"

    def __init__(
        self,
        directory: str | os.PathLike[str],
        source: str | os.PathLike[str] | None = None,
        cold: str | os.PathLike[str] | None = None,
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.root = self.directory.resolve()

        self.catalog_path = self.root / "catalog.sqlite3"
        self.hot_path = self.root / "hot.sqlite3"
        self.backup_dir = self.root / "backups"

        # Conventional source/receiving cold volumes.  They may alternatively
        # be mounted outside the catalog directory.
        self.source_path = Path(source).resolve() if source else self.root / "S"
        self.cold_path = Path(cold).resolve() if cold else self.root / "C"
        # Absolute string handles are convenient for callers and hooks.
        self.S = VolumeHandle("S", self.source_path)
        self.C = VolumeHandle("C", self.cold_path)

        self._init_catalog()
        self._init_hot()

    def close(self) -> None:
        """Close resources.  Connections are short-lived, so this is a no-op."""

    def __enter__(self) -> "Lake":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Connection/schema helpers
    # ------------------------------------------------------------------

    def _catalog_connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.catalog_path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _hot_connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.hot_path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=DELETE")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
                receipt TEXT NOT NULL,
                idx INTEGER NOT NULL,
                source_json TEXT NOT NULL,
                liters INTEGER,
                PRIMARY KEY (receipt, idx)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS hot_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS hot_receipts (
                receipt TEXT PRIMARY KEY,
                row_count INTEGER NOT NULL,
                row_hash TEXT NOT NULL
            )
            """
        )
        con.commit()
        return con

    def _init_catalog(self) -> None:
        with closing(self._catalog_connect()) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS lake_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS freezes (
                    b_key TEXT PRIMARY KEY,
                    b_display TEXT,
                    row_hash TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS admissions (
                    a TEXT PRIMARY KEY,
                    b_key TEXT NOT NULL UNIQUE,
                    row_hash TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS receipts (
                    r TEXT PRIMARY KEY,
                    row_hash TEXT NOT NULL UNIQUE,
                    row_count INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    expired INTEGER NOT NULL DEFAULT 0,
                    expired_at TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS receipt_volumes (
                    r TEXT NOT NULL,
                    role TEXT NOT NULL,
                    path TEXT NOT NULL,
                    row_hash TEXT NOT NULL,
                    PRIMARY KEY (r, role)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS backups (
                    k_key TEXT PRIMARY KEY,
                    k_display TEXT,
                    path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS recovery_points (
                    g TEXT PRIMARY KEY,
                    k_key TEXT NOT NULL,
                    r TEXT NOT NULL,
                    released INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS permits (
                    q_key TEXT PRIMARY KEY,
                    q_display TEXT,
                    r TEXT NOT NULL,
                    v TEXT NOT NULL,
                    token TEXT NOT NULL UNIQUE,
                    purged INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    purged_at TEXT
                )
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_permits_rv ON permits(r, v)"
            )
            con.commit()

    def _init_hot(self) -> None:
        with closing(self._hot_connect()) as con:
            con.execute("PRAGMA integrity_check")
            con.commit()

    # ------------------------------------------------------------------
    # Small catalog helpers
    # ------------------------------------------------------------------

    def _get_receipt(self, con: sqlite3.Connection, r: str) -> sqlite3.Row:
        row = con.execute(
            "SELECT * FROM receipts WHERE r=?", (r,)
        ).fetchone()
        if row is None:
            raise LakeNotFound(f"receipt {r!r}")
        return row

    def _volume_refs(
        self, con: sqlite3.Connection, r: str, row_hash: str | None = None
    ) -> list[tuple[str, Path, str]]:
        rows = con.execute(
            "SELECT role, path, row_hash FROM receipt_volumes WHERE r=? ORDER BY rowid",
            (r,),
        ).fetchall()
        if rows:
            return [(row["role"], Path(row["path"]), row["row_hash"]) for row in rows]

        expected_hash = row_hash or ""
        return [
            ("S", self.source_path, expected_hash),
            ("C", self.cold_path, expected_hash),
        ]

    def _canonical_volume(
        self, v: Any
    ) -> tuple[str, Path]:
        if isinstance(v, VolumeHandle):
            return v.role, Path(os.fspath(v))

        if isinstance(v, str) and v.upper() in VOLUME_NAMES:
            role = v.upper()
            path = self.root / role
            return role, path

        if isinstance(v, (str, os.PathLike)):
            raw = os.fspath(v)
            if isinstance(raw, bytes):
                raw = os.fsdecode(raw)
            path = Path(raw).expanduser()
            if not path.is_absolute():
                path = self.root / path
            path = path.resolve()

            for role, default in (("S", self.source_path), ("C", self.cold_path)):
                if path == default:
                    return role, default

            # Allow a trailing/leading conventional component, while keeping
            # permits bound to a concrete path.
            if path.name in VOLUME_NAMES and path.parent == self.root:
                return path.name, path
            return path.name or "V", path

        raise LakeRejected("v must be a volume identifier or path")

    def _permit_exists_for_volume(
        self, con: sqlite3.Connection, r: str, path: Path
    ) -> bool:
        row = con.execute(
            "SELECT 1 FROM permits WHERE r=? AND v=? LIMIT 1",
            (r, str(path)),
        ).fetchone()
        return row is not None

    def _inspect_volume(
        self,
        con: sqlite3.Connection,
        r: str,
        role: str,
        path: Path,
        expected_hash: str,
        *,
        check_spent: bool = True,
    ) -> tuple[str, list[tuple[Any, int | None]] | None]:
        """Return ('ok', rows), ('spent', None), or a failure status."""

        if check_spent and self._permit_exists_for_volume(con, r, path):
            return "spent", None

        if not path.exists() or not path.is_dir():
            return "missing", None

        body_path = path / f"{expected_hash}.json"
        if not body_path.is_file():
            return "missing", None

        try:
            with body_path.open("r", encoding="utf-8") as handle:
                body = json.load(handle)
            raw_rows = body["rows"]
            rows = _normalize_rows(raw_rows)
            actual_hash = _rows_hash(rows)
            if actual_hash != expected_hash:
                return "mismatch", None
            if body.get("row_hash") != expected_hash:
                return "mismatch", None
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            return "corrupt", None

        return "ok", rows

    def _qualified_volumes(
        self, con: sqlite3.Connection, r: str
    ) -> list[tuple[str, Path, list[tuple[Any, int | None]]]]:
        receipt = self._get_receipt(con, r)
        result = []
        for role, path, volume_hash in self._volume_refs(con, r, receipt["row_hash"]):
            if volume_hash != receipt["row_hash"]:
                continue
            status, rows = self._inspect_volume(
                con, r, role, path, receipt["row_hash"]
            )
            if status == "ok" and rows is not None:
                result.append((role, path, rows))
        return result

    # ------------------------------------------------------------------
    # Freeze / admit / seal / read
    # ------------------------------------------------------------------

    def freeze(self, b: Any, rows: Iterable[Any]) -> Any:
        """Freeze the original row order under caller identifier *b*.

        Repeating a freeze with the same identifier and exactly equal rows is
        idempotent and returns *b*.  Equal identifier/different rows is
        rejected.
        """

        normalized = _normalize_rows(rows)
        row_hash = _rows_hash(normalized)
        b_key = _json_key(b)

        with closing(self._catalog_connect()) as con:
            existing = con.execute(
                "SELECT row_hash, row_count FROM freezes WHERE b_key=?",
                (b_key,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["row_hash"] != row_hash
                    or existing["row_count"] != len(normalized)
                ):
                    raise LakeRejected(
                        f"同号异件，拒绝: freeze {b!r} already contains different evidence"
                    )
                return b

            con.execute(
                "INSERT INTO freezes VALUES (?, ?, ?, ?, ?)",
                (b_key, str(b), row_hash, len(normalized), _now()),
            )
            con.commit()

        return b

    def _write_volume_bodies(
        self, rows: Sequence[tuple[Any, int | None]], row_hash: str
    ) -> None:
        plain = [[source, liters] for source, liters in rows]
        for role, path in (("S", self.source_path), ("C", self.cold_path)):
            path.mkdir(parents=True, exist_ok=True)
            body = {
                "format": "lake-volume-v1",
                "role": role,
                "row_hash": row_hash,
                "rows": plain,
            }
            fd, tmp_name = tempfile.mkstemp(
                prefix=".body-", suffix=".tmp", dir=str(path)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(body, handle, ensure_ascii=False, separators=(",", ":"))
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_name, path / f"{row_hash}.json")
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
                raise

    def admit(self, b: Any, rows: Iterable[Any]) -> str:
        """Admit a whole item only when every row matches the freeze."""

        normalized = _normalize_rows(rows)
        row_hash = _rows_hash(normalized)
        b_key = _json_key(b)

        with closing(self._catalog_connect()) as con:
            frozen = con.execute(
                "SELECT row_hash FROM freezes WHERE b_key=?", (b_key,)
            ).fetchone()
            if frozen is None:
                raise LakeNotFound(f"freeze {b!r}")
            if frozen["row_hash"] != row_hash:
                raise LakeRejected("逐行相符校验失败，拒绝接纳整件")

            existing = con.execute(
                "SELECT a FROM admissions WHERE b_key=?", (b_key,)
            ).fetchone()
            if existing is not None:
                return existing["a"]

            # The two long-term volumes are the only persistent stores of the
            # complete row body.  Write them before publishing admission.
            self._write_volume_bodies(normalized, row_hash)

            a = _new_id("a")
            con.execute(
                "INSERT INTO admissions VALUES (?, ?, ?, ?, ?)",
                (a, b_key, row_hash, len(normalized), _now()),
            )
            con.commit()
            return a

    def _read_hot_rows(
        self, con: sqlite3.Connection, r: str
    ) -> list[tuple[Any, int | None]] | None:
        rows = con.execute(
            "SELECT source_json, liters FROM entries WHERE receipt=? ORDER BY idx",
            (r,),
        ).fetchall()
        if rows:
            result = []
            for row in rows:
                source = json.loads(row["source_json"])
                result.append((source, row["liters"]))
            return result

        marker = con.execute(
            "SELECT 1 FROM hot_receipts WHERE receipt=?", (r,)
        ).fetchone()
        if marker is not None:
            return []
        return None

    def _replace_hot_rows(
        self,
        con: sqlite3.Connection,
        r: str,
        rows: Sequence[tuple[Any, int | None]],
        expected_hash: str,
    ) -> None:
        existing = self._read_hot_rows(con, r)
        if existing is not None:
            if _rows_hash(existing) != expected_hash or len(existing) != len(rows):
                raise LakeRejected(
                    f"hot ledger receipt {r!r} contains non-matching rows"
                )
            return

        con.execute(
            """
            INSERT INTO hot_receipts(receipt,row_count,row_hash)
            VALUES (?,?,?)
            """,
            (r, len(rows), expected_hash),
        )
        con.executemany(
            "INSERT INTO entries(receipt, idx, source_json, liters) VALUES (?, ?, ?, ?)",
            [
                (r, index, json.dumps(source, ensure_ascii=False), liters)
                for index, (source, liters) in enumerate(rows)
            ],
        )

    def seal(self, a: str) -> str:
        """Sign/admission *a* and publish its fixed receipt in the hot ledger."""

        with closing(self._catalog_connect()) as con:
            admission = con.execute(
                "SELECT * FROM admissions WHERE a=?", (a,)
            ).fetchone()
            if admission is None:
                raise LakeNotFound(f"admission {a!r}")
            row_hash = admission["row_hash"]
            count = admission["row_count"]

            # Recover the body from one of the two long-term volumes.
            rows: list[tuple[Any, int | None]] | None = None
            for role, path in (("S", self.source_path), ("C", self.cold_path)):
                status, candidate = self._inspect_volume(
                    con, "__seal__", role, path, row_hash, check_spent=False
                )
                if status == "ok" and candidate is not None:
                    rows = candidate
                    break
            if rows is None:
                raise LakeRejected("cannot seal: long-term volume body is missing")
            if len(rows) != count or _rows_hash(rows) != row_hash:
                raise LakeRejected("cannot seal: volume body mismatch")

            r = _receipt_for_hash(row_hash)
            total = sum(liters for _, liters in rows if liters is not None)

            with closing(self._hot_connect()) as hot:
                hot.execute("BEGIN IMMEDIATE")
                self._replace_hot_rows(hot, r, rows, row_hash)
                hot.execute(
                    "INSERT OR REPLACE INTO hot_meta(key,value) VALUES ('last_receipt', ?)",
                    (r,),
                )
                hot.commit()

            con.execute(
                """
                INSERT INTO receipts(r,row_hash,row_count,total,created_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(r) DO UPDATE SET
                    row_hash=excluded.row_hash,
                    row_count=excluded.row_count,
                    total=excluded.total
                """,
                (r, row_hash, len(rows), total, _now()),
            )
            for role, path in (("S", self.source_path), ("C", self.cold_path)):
                con.execute(
                    """
                    INSERT OR REPLACE INTO receipt_volumes(r,role,path,row_hash)
                    VALUES (?,?,?,?)
                    """,
                    (r, role, str(path), row_hash),
                )
            con.commit()
            return r

    def _is_expired(self, con: sqlite3.Connection, r: str) -> bool:
        receipt = self._get_receipt(con, r)
        if receipt["expired"]:
            return True

        refs = self._volume_refs(con, r, receipt["row_hash"])
        both_deleted = all(not Path(path).exists() for _, path, _ in refs)
        if not both_deleted:
            return False

        # Once both long-term volumes are gone the account is due/expired.
        # release(g) is the preservation-lifecycle operation and also calls
        # this settlement hook when its second volume has already gone.
        con.execute(
            "UPDATE receipts SET expired=1, expired_at=? WHERE r=?",
            (_now(), r),
        )
        con.commit()
        return True

    def read(self, r: str) -> tuple[Any, ...]:
        """Read ``(r, original_rows, non_null_sum)`` from the hot ledger.

        Once a released recovery point has had both volumes purged, the result
        is exactly ``(r, "已到期")``.
        """

        with closing(self._catalog_connect()) as con:
            receipt = self._get_receipt(con, r)
            if self._is_expired(con, r):
                return (r, "已到期")
            expected_hash = receipt["row_hash"]

        with closing(self._hot_connect()) as hot:
            rows = self._read_hot_rows(hot, r)

        if rows is None:
            raise LakeNotFound(f"receipt {r!r} is not in hot ledger")
        if _rows_hash(rows) != expected_hash:
            raise LakeRejected(f"hot ledger receipt {r!r} is mismatched")

        total = sum(liters for _, liters in rows if liters is not None)
        return (r, rows, total)

    # ------------------------------------------------------------------
    # Backup / protect / release
    # ------------------------------------------------------------------

    def backup_path(self, k: Any) -> Path:
        """Return the concrete backup location for key/path *k*."""

        if isinstance(k, os.PathLike):
            path = Path(os.fspath(k))
            return path if path.is_absolute() else self.root / path

        if isinstance(k, str) and (
            "/" in k
            or os.sep in k
            or k.lower().endswith((".db", ".bak", ".sqlite", ".sqlite3"))
        ):
            path = Path(k)
            return path if path.is_absolute() else self.root / path

        k_key = _json_key(k)
        digest = hashlib.sha256(k_key.encode("utf-8")).hexdigest()[:20]
        return self.backup_dir / f"{digest}.sqlite3"

    def backup(self, k: Any) -> Any:
        """Create an online SQLite backup of the current hot ledger.

        Returns the caller's key for convenient chaining.
        """

        target = self.backup_path(k)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".backup-", suffix=".tmp", dir=str(target.parent)
        )
        os.close(fd)
        try:
            with closing(self._hot_connect()) as source, closing(
                sqlite3.connect(tmp_name)
            ) as destination:
                source.backup(destination)
                check = destination.execute("PRAGMA integrity_check").fetchone()
                if check[0] != "ok":
                    raise LakeRejected("backup integrity check failed")

            os.replace(tmp_name, target)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

        digest = _sha256_file(target)
        with closing(self._catalog_connect()) as con:
            con.execute(
                """
                INSERT INTO backups(k_key,k_display,path,sha256,created_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(k_key) DO UPDATE SET
                    k_display=excluded.k_display,
                    path=excluded.path,
                    sha256=excluded.sha256,
                    created_at=excluded.created_at
                """,
                (_json_key(k), str(k), str(target), digest, _now()),
            )
            con.commit()
        return k

    def _verified_backup(
        self, con: sqlite3.Connection, k: Any
    ) -> sqlite3.Row:
        row = con.execute(
            "SELECT * FROM backups WHERE k_key=?", (_json_key(k),)
        ).fetchone()
        if row is None:
            raise LakeNotFound(f"backup {k!r}")

        path = Path(row["path"])
        if not path.is_file():
            raise LakeRejected(f"backup {k!r} evidence is missing")
        if _sha256_file(path) != row["sha256"]:
            raise LakeRejected(f"backup {k!r} checksum mismatch")

        uri = path.resolve().as_uri() + "?mode=ro"
        try:
            with closing(sqlite3.connect(uri, uri=True)) as backup_con:
                result = backup_con.execute("PRAGMA integrity_check").fetchone()
                if result[0] != "ok":
                    raise LakeRejected(f"backup {k!r} is corrupt")
        except sqlite3.DatabaseError as exc:
            raise LakeRejected(f"backup {k!r} cannot be verified") from exc
        return row

    def protect(self, k: Any, r: str) -> str:
        """Verify backup/receipt/volume evidence and register a recovery point."""

        with closing(self._catalog_connect()) as con:
            receipt = self._get_receipt(con, r)
            self._verified_backup(con, k)
            candidates = self._qualified_volumes(con, r)
            if not candidates:
                raise LakeRejected(
                    "cannot protect: at least one qualified volume is required"
                )

            g = _new_id("g")
            con.execute(
                "INSERT INTO recovery_points(g,k_key,r,created_at) VALUES (?,?,?,?)",
                (g, _json_key(k), r, _now()),
            )
            con.commit()
            return g

    def release(self, g: str) -> None:
        """Release the preservation hold for recovery point *g*."""

        with closing(self._catalog_connect()) as con:
            row = con.execute(
                "SELECT * FROM recovery_points WHERE g=?", (g,)
            ).fetchone()
            if row is None:
                raise LakeNotFound(f"recovery point {g!r}")
            con.execute(
                "UPDATE recovery_points SET released=1 WHERE g=?", (g,)
            )
            con.commit()
            self._is_expired(con, row["r"])

    # ------------------------------------------------------------------
    # Permits / purge
    # ------------------------------------------------------------------

    def permit(
        self, q: Any, r: str, v: Any
    ) -> tuple[str, bool]:
        """Issue/reopen the permit for (q, r, v).

        Returns ``(permit_token, already_deleted)``.  The first issue spends
        the volume qualification immediately, even before purge removes files.
        """

        q_key = _json_key(q)
        role, path = self._canonical_volume(v)
        v_key = str(path)

        with closing(self._catalog_connect()) as con:
            existing = con.execute(
                "SELECT * FROM permits WHERE q_key=?", (q_key,)
            ).fetchone()
            if existing is not None:
                if existing["r"] != r or existing["v"] != v_key:
                    raise LakeRejected(
                        f"permit key {q!r} was opened with different parameters"
                    )
                deleted = bool(existing["purged"]) or not path.exists()
                return existing["token"], deleted

            receipt = self._get_receipt(con, r)

            prior = con.execute(
                "SELECT q_key FROM permits WHERE r=? AND v=? LIMIT 1",
                (r, v_key),
            ).fetchone()
            if prior is not None:
                raise LakeRejected(
                    f"volume {role} for {r!r} qualification was already issued"
                )

            status, rows = self._inspect_volume(
                con, r, role, path, receipt["row_hash"], check_spent=False
            )
            if status != "ok" or rows is None:
                raise LakeRejected(
                    f"volume {role} is not qualified for {r!r}: {status}"
                )

            token = _new_id("permit")
            con.execute(
                """
                INSERT INTO permits(q_key,q_display,r,v,token,created_at)
                VALUES (?,?,?,?,?,?)
                """,
                (q_key, str(q), r, v_key, token, _now()),
            )
            con.commit()
            return token, False

    def _find_permit(
        self, con: sqlite3.Connection, permit_or_q: Any
    ) -> sqlite3.Row | None:
        # Tolerate callers that pass the whole (permit, already_deleted) tuple.
        if (
            isinstance(permit_or_q, tuple)
            and len(permit_or_q) == 2
            and isinstance(permit_or_q[0], str)
        ):
            permit_or_q = permit_or_q[0]

        value = str(permit_or_q)
        row = con.execute(
            "SELECT * FROM permits WHERE token=?", (value,)
        ).fetchone()
        if row is not None:
            return row
        return con.execute(
            "SELECT * FROM permits WHERE q_key=?", (_json_key(permit_or_q),)
        ).fetchone()

    def purge(self, permit: str | Any) -> None:
        """Actually delete the volume named by *permit* and settle the permit."""

        with closing(self._catalog_connect()) as con:
            row = self._find_permit(con, permit)
            if row is None:
                raise LakeNotFound("permit")

            path = Path(row["v"])
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.exists() and path.is_dir():
                shutil.rmtree(path)
            elif path.exists():  # pragma: no cover - unusual filesystem node
                path.unlink()

            con.execute(
                "UPDATE permits SET purged=1, purged_at=? WHERE q_key=?",
                (_now(), row["q_key"]),
            )
            con.commit()
            self._is_expired(con, row["r"])

    # ------------------------------------------------------------------
    # Rollback simulation and restore
    # ------------------------------------------------------------------

    def rollback_hot(self, k: Any | None = None) -> None:
        """External rollback helper used by demos/verification.

        With no key, remove the hot ledger.  With a backup key, replace the
        hot ledger by that backup image (without re-adding sealed receipts).
        """

        if k is None:
            for suffix in ("", "-wal", "-shm", "-journal"):
                p = Path(str(self.hot_path) + suffix)
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
            self._init_hot()
            return

        backup = self.backup_path(k)
        if not backup.is_file():
            raise LakeNotFound(f"backup {k!r}")
        self.backup_dir.mkdir(exist_ok=True)

        fd, tmp_name = tempfile.mkstemp(
            prefix=".rollback-", suffix=".tmp", dir=str(self.root)
        )
        os.close(fd)
        try:
            with closing(sqlite3.connect(backup)) as src, closing(
                sqlite3.connect(tmp_name)
            ) as dst:
                src.backup(dst)
            os.replace(tmp_name, self.hot_path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise
        self._init_hot()

    def restore(
        self,
        k: Any,
        g: str,
        r: str,
        hook: Callable[[str], Any] | None = None,
    ) -> str | None:
        """Verify the binding and atomically restore backup + full *r* account.

        *hook* is invoked once after the first qualified volume is selected,
        before any restored result is published to the hot ledger.
        """

        k_key = _json_key(k)

        # Phase 1: verify all non-mutable binding evidence.
        with closing(self._catalog_connect()) as con:
            point = con.execute(
                "SELECT * FROM recovery_points WHERE g=?", (g,)
            ).fetchone()
            if point is None:
                raise LakeNotFound(f"recovery point {g!r}")
            if point["k_key"] != k_key or point["r"] != r:
                raise LakeRejected("错配，整次拒绝: k/g/r recovery binding mismatch")

            receipt = self._get_receipt(con, r)
            expected_hash = receipt["row_hash"]
            self._verified_backup(con, k)

            candidates = self._qualified_volumes(con, r)
            if not candidates:
                raise LakeRejected(
                    "证据不足，整次拒绝: no qualified recovery volume"
                )
            first_role, first_path, _ = candidates[0]

        # Publish the selection, but explicitly do not publish a restore yet.
        selected_handle: Any = VolumeHandle(first_role, first_path)
        if hook is not None:
            hook(selected_handle)

        # Phase 2: re-read committed permit/purge state after the callback.
        with closing(self._catalog_connect()) as con:
            point = con.execute(
                "SELECT * FROM recovery_points WHERE g=?", (g,)
            ).fetchone()
            if point is None or point["k_key"] != k_key or point["r"] != r:
                raise LakeRejected("recovery point binding changed during hook")

            receipt = self._get_receipt(con, r)
            if receipt["row_hash"] != expected_hash:
                raise LakeRejected("receipt evidence changed during hook")

            candidates = self._qualified_volumes(con, r)
            if not candidates:
                raise LakeRejected(
                    "证据不足，整次拒绝: selected volume was cleared and "
                    "no other qualified volume remains"
                )

            # Prefer the callback-selected volume if it remains qualified;
            # otherwise fall through to another qualified volume.
            chosen = None
            for role, path, rows in candidates:
                if path == first_path:
                    chosen = (role, path, rows)
                    break
            if chosen is None:
                chosen = candidates[0]
            role, volume_path, rows = chosen

            if _rows_hash(rows) != expected_hash:
                raise LakeRejected("selected volume rows are mismatched")
            if len(rows) != receipt["row_count"]:
                raise LakeRejected("selected volume is not a complete item")

            backup_path = self._verified_backup(con, k)["path"]

        # Phase 3: stage backup + receipt in a separate database.  Any failure
        # leaves the current hot ledger untouched.
        fd, stage_name = tempfile.mkstemp(
            prefix=".restore-stage-", suffix=".sqlite3", dir=str(self.root)
        )
        os.close(fd)
        try:
            with closing(sqlite3.connect(backup_path)) as source, closing(
                sqlite3.connect(stage_name)
            ) as staged:
                staged.row_factory = sqlite3.Row
                source.backup(staged)
                staged.execute(
                    """
                    CREATE TABLE IF NOT EXISTS entries (
                        receipt TEXT NOT NULL,
                        idx INTEGER NOT NULL,
                        source_json TEXT NOT NULL,
                        liters INTEGER,
                        PRIMARY KEY (receipt, idx)
                    )
                    """
                )
                staged.execute(
                    """
                    CREATE TABLE IF NOT EXISTS hot_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                    """
                )
                staged.execute(
                    """
                    CREATE TABLE IF NOT EXISTS hot_receipts (
                        receipt TEXT PRIMARY KEY,
                        row_count INTEGER NOT NULL,
                        row_hash TEXT NOT NULL
                    )
                    """
                )

                existing = self._read_hot_rows(staged, r)
                if existing is not None:
                    if (
                        _rows_hash(existing) != expected_hash
                        or len(existing) != len(rows)
                    ):
                        raise LakeRejected(
                            "backup receipt evidence does not match qualified volume"
                        )
                else:
                    staged.execute(
                        """
                        INSERT INTO hot_receipts(receipt,row_count,row_hash)
                        VALUES (?,?,?)
                        """,
                        (r, len(rows), expected_hash),
                    )
                    staged.executemany(
                        """
                        INSERT INTO entries(receipt, idx, source_json, liters)
                        VALUES (?, ?, ?, ?)
                        """,
                        [
                            (
                                r,
                                index,
                                json.dumps(source, ensure_ascii=False),
                                liters,
                            )
                            for index, (source, liters) in enumerate(rows)
                        ],
                    )
                staged.execute(
                    "INSERT OR REPLACE INTO hot_meta(key,value) VALUES ('restored_g', ?)",
                    (g,),
                )
                check = staged.execute("PRAGMA integrity_check").fetchone()
                if check[0] != "ok":
                    raise LakeRejected("staged restore failed integrity check")
                staged.commit()

            # Phase 4: atomic publication.  Hook has already returned, so no
            # uncommitted restore was visible inside the callback.
            os.replace(stage_name, self.hot_path)
        except BaseException:
            try:
                os.unlink(stage_name)
            except FileNotFoundError:
                pass
            raise

        # Ensure schema pragmas / a fresh connection see the replacement.
        with closing(self._hot_connect()) as con:
            con.execute("PRAGMA integrity_check")

        return None


class VolumeHandle(str):
    """Absolute string handle for a selected volume.

    It remains a normal string when passed back to Lake, while also exposing
    :class:`pathlib.Path`-style helpers to hook/test code.
    """

    role: str
    path: Path

    def __new__(cls, role: str, path: str | os.PathLike[str] | Path) -> "VolumeHandle":
        concrete = Path(os.fspath(path)).expanduser().resolve()
        obj = super().__new__(cls, str(concrete))
        obj.role = role
        obj.path = concrete
        return obj

    def __fspath__(self) -> str:
        return str(self.path)

    def __truediv__(self, other: Any) -> Path:
        return self.path / other

    def __getattr__(self, name: str) -> Any:
        return getattr(self.path, name)
