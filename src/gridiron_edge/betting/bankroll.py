# src/gridiron_edge/betting/bankroll.py
"""Bankroll transaction log - tracks every dollar in and out.

Decoupled from the bet ledger. The CLI layer orchestrates calls to both
``ledger.py`` and ``bankroll.py`` so that bet placement deducts stake
and settlement credits the gross return.

Transaction types::

    deposit       Money added to the bankroll.
    withdraw      Money removed from the bankroll.
    bet_placed    Stake leaves the bankroll (placed a bet).
    bet_settled   Gross return enters the bankroll (won/push payout).

The complete log is rewritten and published atomically on every write via
a colocated temporary file and rename, so an interruption during
serialization or publication leaves the prior log unchanged. This
provides atomically visible publication only -- it does not coordinate
concurrent writers. No same-process concurrent-writer risk has been
confirmed for this module (contrast ``betting/ledger.py``, which required
a ``threading.RLock`` for a confirmed thread-pool-routed API caller); if
such a risk is later confirmed here, that coordination must be added as
its own change, not assumed present.

Storage lives at ``data/betting/bankroll_txn.parquet``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import logging
from logging import Logger
import os
from pathlib import Path
from typing import Final, Literal
import uuid

import pandas as pd
from pandas import DataFrame

logger: Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

TxnType: type[TxnType] = Literal["deposit", "withdraw", "bet_placed", "bet_settled"]

_INFLOWS: frozenset[str] = frozenset({"deposit", "bet_settled"})
_OUTFLOWS: frozenset[str] = frozenset({"withdraw", "bet_placed"})

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_TXN_COLUMNS: Final[list[str]] = [
    "txn_id",
    "timestamp",
    "txn_type",
    "amount",
    "reference_id",
    "note",
]

# ---------------------------------------------------------------------------
# Bankroll evidence
# ---------------------------------------------------------------------------

_BANKROLL_SOURCE_KIND: Final[str] = "bankroll_transaction_ledger"


@dataclass(frozen=True, slots=True)
class BankrollSnapshot:
    """Bankroll evidence derived from transaction rows visible at a cutoff.

    Owned by this module, not by any consumer's domain. Callers adapt this
    into their own evidence type at their own composition boundary; this
    module does not know or depend on any consumer's shape.

    ``source_id`` is a digest of every balance-material field on every
    visible transaction (``txn_id``, ``timestamp``, ``txn_type``,
    ``amount``, ``reference_id``) plus the cutoff. The free-text ``note``
    field is intentionally excluded -- it does not contribute to the
    signed balance or to transaction linkage, so it is not treated as
    material to bankroll identity.
    """

    amount: float
    observed_at: datetime
    source_kind: str
    source_id: str


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _txn_path(repo: Path | None = None) -> Path:
    """Return the path to the bankroll transaction log.

    Creates the parent directory if it does not exist.

    Args:
        repo: Repository root override.

    Returns:
        Absolute path to ``data/betting/bankroll_txn.parquet``.
    """
    if repo is None:
        from gridiron_edge.core.settings import get_settings

        repo = get_settings().repo_root
    path: Path = repo / "data" / "betting" / "bankroll_txn.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _empty_txn_log() -> pd.DataFrame:
    """Return an empty DataFrame with the correct transaction schema."""
    return pd.DataFrame(columns=_TXN_COLUMNS)


def _read_txn_log(repo: Path | None = None) -> pd.DataFrame:
    """Read the transaction log from disk.

    Returns an empty DataFrame with the correct schema if the file does
    not exist.
    """
    path: Path = _txn_path(repo)
    if not path.exists():
        return _empty_txn_log()
    df: DataFrame = pd.read_parquet(path)
    for col in _TXN_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df.loc[:, _TXN_COLUMNS]


def _write_txn_log(df: pd.DataFrame, repo: Path | None = None) -> Path:
    """Write the transaction log to disk.

    Serializes the complete log to a colocated temporary file, then
    publishes it via an atomic rename. An interruption during
    serialization or before the rename leaves the previously published
    log unchanged; a reader never observes a partially written file. This
    provides atomically visible publication only -- it does not
    coordinate two overlapping writers (see the module docstring).
    """
    path: Path = _txn_path(repo)
    temporary: Path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        df.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


# ---------------------------------------------------------------------------
# Bankroll-evidence helpers
# ---------------------------------------------------------------------------


def _require_utc(value: datetime, *, label: str) -> datetime:
    """Require one timestamp to be timezone-aware UTC."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be timezone-aware UTC.")
    return value


def _digest(value: object) -> str:
    """Return a canonical SHA-256 digest of one JSON-serializable value."""
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return sha256(encoded).hexdigest()


# ---------------------------------------------------------------------------
# Sign convention
# ---------------------------------------------------------------------------


def signed_amount(txn_type: str, amount: float) -> float:
    """Return the signed amount for balance calculations.

    Deposits and bet settlements are positive (money in).
    Withdrawals and bet placements are negative (money out).
    """
    if txn_type in _INFLOWS:
        return amount
    return -amount


def _signed_amount_series(
    txn_types: pd.Series,
    amounts: pd.Series,
) -> pd.Series:
    """Vectorized version of ``signed_amount`` for a Series of transactions.

    Mirrors the scalar logic: inflows return positive, outflows return
    negative. Used by ``current_balance`` and ``balance_history`` to
    avoid row-wise apply (bankroll/H1).
    """
    import numpy as np

    return pd.Series(
        np.where(txn_types.isin(_INFLOWS), amounts, -amounts),
        index=txn_types.index,
    )


# ---------------------------------------------------------------------------
# Internal append helper
# ---------------------------------------------------------------------------


def _append_txn(
    txn_type: TxnType,
    amount: float,
    *,
    reference_id: str | None = None,
    note: str | None = None,
    repo: Path | None = None,
) -> str:
    """Append a single transaction to the log. Returns the txn_id.

    Args:
        txn_type: One of ``"deposit"``, ``"withdraw"``, ``"bet_placed"``,
            ``"bet_settled"``.
        amount: Transaction amount (must be >= 0).
        reference_id: Optional reference (e.g. bet_id).
        note: Optional human-readable note.
        repo: Repository root override.

    Returns:
        The generated ``txn_id`` (UUID string).

    Raises:
        ValueError: If ``amount`` is negative.
    """
    if amount < 0:
        msg: str = f"Transaction amount must be >= 0, got {amount}"
        raise ValueError(msg)

    txn_id = str(uuid.uuid4())
    row: dict[str, datetime | float | str | None] = {
        "txn_id": txn_id,
        "timestamp": datetime.now(UTC),
        "txn_type": txn_type,
        "amount": amount,
        "reference_id": reference_id,
        "note": note,
    }

    new_row = pd.DataFrame([row], columns=_TXN_COLUMNS)
    existing: DataFrame = _read_txn_log(repo)

    if existing.empty:
        combined: DataFrame = new_row
    else:
        combined = pd.concat(
            [existing.dropna(axis=1, how="all"), new_row.dropna(axis=1, how="all")],
            ignore_index=True,
        ).reindex(columns=_TXN_COLUMNS)

    _write_txn_log(combined, repo)
    logger.info("Txn %s: %s %.2f", txn_id, txn_type, amount)
    return txn_id


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def deposit(
    amount: float,
    *,
    note: str | None = None,
    repo: Path | None = None,
) -> str:
    """Record a deposit (money added to bankroll).

    Args:
        amount: Deposit amount (must be > 0).
        note: Optional note.
        repo: Repository root override.

    Returns:
        Transaction ID.

    Raises:
        ValueError: If ``amount`` is not positive.
    """
    if amount <= 0:
        msg: str = f"Deposit amount must be positive, got {amount}"
        raise ValueError(msg)
    return _append_txn("deposit", amount, note=note, repo=repo)


def withdraw(
    amount: float,
    *,
    note: str | None = None,
    repo: Path | None = None,
) -> str:
    """Record a withdrawal (money removed from bankroll).

    Does **not** check whether the balance would go negative - that is
    a CLI-layer concern, not a library concern.

    Args:
        amount: Withdrawal amount (must be > 0).
        note: Optional note.
        repo: Repository root override.

    Returns:
        Transaction ID.

    Raises:
        ValueError: If ``amount`` is not positive.
    """
    if amount <= 0:
        msg: str = f"Withdrawal amount must be positive, got {amount}"
        raise ValueError(msg)
    return _append_txn("withdraw", amount, note=note, repo=repo)


def record_bet_placed(
    stake: float,
    *,
    bet_id: str | None = None,
    repo: Path | None = None,
) -> str:
    """Record a bet placement (stake leaves the bankroll).

    Args:
        stake: Amount wagered (must be > 0).
        bet_id: The bet's UUID for cross-referencing.
        repo: Repository root override.

    Returns:
        Transaction ID.

    Raises:
        ValueError: If ``stake`` is not positive.
    """
    if stake <= 0:
        msg: str = f"Stake must be positive, got {stake}"
        raise ValueError(msg)
    return _append_txn(
        "bet_placed",
        stake,
        reference_id=bet_id,
        note=f"Bet placed: {bet_id}",
        repo=repo,
    )


def record_bet_settled(
    stake: float,
    pnl: float,
    *,
    bet_id: str | None = None,
    repo: Path | None = None,
) -> str:
    """Record a bet settlement (gross return enters the bankroll).

    The gross return is ``stake + pnl``:

    - **Won:** ``stake + profit`` (e.g. 100 + 150 = 250)
    - **Lost:** ``stake + (-stake) = 0`` (nothing returns)
    - **Push:** ``stake + 0 = stake`` (original stake returns)

    If the gross return is zero (a loss), the transaction is still
    recorded for audit trail purposes.

    Args:
        stake: Original stake amount.
        pnl: Profit/loss from ``compute_pnl()``.
        bet_id: The bet's UUID for cross-referencing.
        repo: Repository root override.

    Returns:
        Transaction ID.
    """
    gross_return: float = stake + pnl
    # Clamp to zero (shouldn't happen, but defensive)
    gross_return = max(gross_return, 0.0)
    return _append_txn(
        "bet_settled",
        gross_return,
        reference_id=bet_id,
        note=f"Bet settled: {bet_id} (PnL={pnl:+.2f})",
        repo=repo,
    )


def current_balance(*, repo: Path | None = None) -> float:
    """Compute the current bankroll balance.

    Returns the sum of all signed transactions. Returns ``0.0`` if
    no transactions exist.
    """
    df: DataFrame = _read_txn_log(repo)
    if df.empty:
        return 0.0
    signs = _signed_amount_series(df["txn_type"], df["amount"])
    return float(signs.sum())


def balance_history(*, repo: Path | None = None) -> pd.DataFrame:
    """Build a running balance history.

    Returns a DataFrame sorted by timestamp with columns:
    ``timestamp``, ``txn_type``, ``amount``, ``signed_amount``,
    ``running_balance``.
    """
    df: DataFrame = _read_txn_log(repo)
    if df.empty:
        return pd.DataFrame(
            columns=["timestamp", "txn_type", "amount", "signed_amount", "running_balance"],
        )
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["signed_amount"] = _signed_amount_series(df["txn_type"], df["amount"])
    df["running_balance"] = df["signed_amount"].cumsum()
    return df.loc[:, ["timestamp", "txn_type", "amount", "signed_amount", "running_balance"]]


def load_transactions(
    *,
    txn_type: str | None = None,
    repo: Path | None = None,
) -> pd.DataFrame:
    """Load transactions with optional filter.

    Args:
        txn_type: Filter to this transaction type.
        repo: Repository root override.

    Returns:
        Filtered DataFrame. Empty with correct schema if no matches.
    """
    df: DataFrame = _read_txn_log(repo)
    if df.empty:
        return df
    if txn_type is not None:
        df = df.loc[df["txn_type"] == txn_type, :]
    return df.reset_index(drop=True)


def _require_valid_evidence(transactions: pd.DataFrame) -> None:
    """Require the transaction log to satisfy the evidence contract.

    Before promoting it into recommendation evidence. Several of these
    properties are already enforced by ``_append_txn`` at write time, but
    this function re-validates them at the read boundary, since the
    persisted file itself is not otherwise proven immutable or free of
    external modification (see module docstring).
    """
    if list(transactions.columns) != _TXN_COLUMNS:
        raise ValueError("Bankroll transaction log does not match the canonical schema.")
    if transactions["txn_id"].isna().any():
        raise ValueError("Bankroll transaction log contains a null txn_id.")
    if transactions["txn_id"].duplicated().any():
        raise ValueError("Bankroll transaction log contains duplicate txn_id values.")
    for value in transactions["timestamp"]:
        _require_utc(pd.Timestamp(value).to_pydatetime(), label="transaction timestamp")
    unknown_types = set(transactions["txn_type"]) - _INFLOWS - _OUTFLOWS
    if unknown_types:
        raise ValueError(
            f"Bankroll transaction log contains unknown txn_type values: {sorted(unknown_types)}"
        )
    import numpy as np

    amounts = transactions["amount"].to_numpy(dtype=float)
    if not bool(np.isfinite(amounts).all()) or bool((amounts < 0).any()):
        raise ValueError("Bankroll transaction log contains a non-finite or negative amount.")


def bankroll_snapshot_as_of(
    cutoff: datetime,
    *,
    repo: Path | None = None,
) -> BankrollSnapshot | None:
    """Derive deterministic, content-identified bankroll evidence.

    From transactions visible at an inclusive cutoff. Filters the
    transaction log to rows with ``timestamp <= cutoff``, then computes
    the balance and a content-derived identity from exactly those rows.
    Returns ``None`` when no transaction rows are visible at or before
    the cutoff -- absence of history is not the same fact as a confirmed
    zero balance, and this function does not conflate them.

    This provides deterministic derivation from the transaction rows
    currently recorded as visible at the cutoff, with an exact content-
    derived source identity: post-cutoff transactions never affect the
    result, and if the visible transaction set or any balance-material
    field on a visible row later changes, the resulting amount and
    source_id change accordingly, so a changed evidentiary basis is
    always detectable as a different identity. This function does not,
    by itself, establish immutable historical reproduction: it does not
    enforce that the operational transaction log is append-only, does not
    prevent existing rows from being altered or removed, and does not
    provide any mechanism to reload the exact original transaction set
    later purely from a source_id. Those properties would require either
    hardened, validated append-only guarantees on the ledger itself, or a
    separately persisted, independently reloadable immutable bankroll-
    snapshot artifact -- neither is established by this unit.

    Args:
        cutoff: The exact UTC decision-time boundary.
        repo: Repository root override.

    Returns:
        ``None`` if no transaction rows are visible at or before cutoff.
        Otherwise a ``BankrollSnapshot`` whose amount reflects exactly the
        visible rows, including a genuine ``amount == 0.0`` when those
        rows net to zero.

    Raises:
        ValueError: If the transaction log fails evidence-boundary
            validation (malformed schema, null or duplicate transaction
            IDs, non-UTC timestamps, unknown transaction types, or
            non-finite/negative amounts), or if ``cutoff`` is not
            timezone-aware UTC.
    """
    cutoff_utc = _require_utc(cutoff, label="cutoff")
    transactions = _read_txn_log(repo)
    if transactions.empty:
        return None

    _require_valid_evidence(transactions)

    visible = transactions.loc[transactions["timestamp"] <= cutoff_utc, :].copy()
    if visible.empty:
        return None

    visible = visible.sort_values(["timestamp", "txn_id"], kind="stable").reset_index(drop=True)
    amount = float(_signed_amount_series(visible["txn_type"], visible["amount"]).sum())

    material_rows = [
        {
            "txn_id": str(row["txn_id"]),
            "timestamp": pd.Timestamp(row["timestamp"]).to_pydatetime().isoformat(),
            "txn_type": str(row["txn_type"]),
            "amount": float(row["amount"]),
            "reference_id": (None if pd.isna(row["reference_id"]) else str(row["reference_id"])),
        }
        for _, row in visible.iterrows()
    ]
    source_id = _digest({"cutoff": cutoff_utc.isoformat(), "transactions": material_rows})

    return BankrollSnapshot(amount, cutoff_utc, _BANKROLL_SOURCE_KIND, source_id)
