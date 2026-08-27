# tests/unit/betting/test_bankroll.py
"""Unit tests for bankroll management."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
import uuid as uuid_module
from uuid import UUID

import pandas as pd
import pytest

import gridiron_edge.betting.bankroll as bankroll_module
from gridiron_edge.betting.bankroll import (
    BankrollSnapshot,
    balance_history,
    bankroll_snapshot_as_of,
    current_balance,
    deposit,
    load_transactions,
    record_bet_placed,
    record_bet_settled,
    withdraw,
)

_T0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
_T1 = datetime(2026, 9, 1, 11, 0, tzinfo=UTC)
_T2 = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
_T3 = datetime(2026, 9, 1, 13, 0, tzinfo=UTC)


def _snapshot(repo: Path, cutoff: datetime) -> BankrollSnapshot:
    snapshot = bankroll_snapshot_as_of(cutoff, repo=repo)
    assert snapshot is not None
    return snapshot


# ---------------------------------------------------------------------------
# TestDeposit
# ---------------------------------------------------------------------------


class TestDeposit:
    """Tests for depositing funds."""

    def test_creates_txn_log(self, tmp_path: Path) -> None:
        """First deposit creates the transaction log file."""
        deposit(500.0, repo=tmp_path)
        assert (tmp_path / "data" / "betting" / "bankroll_txn.parquet").exists()

    def test_deposit_amount(self, tmp_path: Path) -> None:
        """Deposit records the correct amount and type."""
        deposit(500.0, repo=tmp_path)
        df = load_transactions(repo=tmp_path)
        assert len(df) == 1
        assert df.iloc[0]["txn_type"] == "deposit"
        assert df.iloc[0]["amount"] == 500.0

    def test_deposit_returns_txn_id(self, tmp_path: Path) -> None:
        """Deposit returns a valid UUID."""
        txn_id = deposit(500.0, repo=tmp_path)
        UUID(txn_id)

    def test_invalid_amount_raises(self, tmp_path: Path) -> None:
        """Deposit with non-positive amount raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            deposit(0.0, repo=tmp_path)
        with pytest.raises(ValueError, match="positive"):
            deposit(-100.0, repo=tmp_path)


# ---------------------------------------------------------------------------
# TestWithdraw
# ---------------------------------------------------------------------------


class TestWithdraw:
    """Tests for withdrawing funds."""

    def test_withdraw_recorded(self, tmp_path: Path) -> None:
        """Withdrawal records correct type and amount."""
        withdraw(200.0, repo=tmp_path)
        df = load_transactions(repo=tmp_path)
        assert len(df) == 1
        assert df.iloc[0]["txn_type"] == "withdraw"
        assert df.iloc[0]["amount"] == 200.0

    def test_withdraw_returns_txn_id(self, tmp_path: Path) -> None:
        """Withdrawal returns a valid UUID."""
        txn_id = withdraw(200.0, repo=tmp_path)
        UUID(txn_id)

    def test_invalid_amount_raises(self, tmp_path: Path) -> None:
        """Withdrawal with non-positive amount raises ValueError."""
        with pytest.raises(ValueError, match="positive"):
            withdraw(0.0, repo=tmp_path)


# ---------------------------------------------------------------------------
# TestRecordBetPlaced
# ---------------------------------------------------------------------------


class TestRecordBetPlaced:
    """Tests for recording bet placements."""

    def test_bet_placed_recorded(self, tmp_path: Path) -> None:
        """Bet placement records correct type and amount."""
        record_bet_placed(100.0, repo=tmp_path)
        df = load_transactions(repo=tmp_path)
        assert len(df) == 1
        assert df.iloc[0]["txn_type"] == "bet_placed"
        assert df.iloc[0]["amount"] == 100.0

    def test_bet_placed_with_reference(self, tmp_path: Path) -> None:
        """Bet placement stores bet_id as reference_id."""
        record_bet_placed(100.0, bet_id="abc-123", repo=tmp_path)
        df = load_transactions(repo=tmp_path)
        assert df.iloc[0]["reference_id"] == "abc-123"

    def test_reduces_balance(self, tmp_path: Path) -> None:
        """Placing a bet reduces the balance."""
        deposit(1000.0, repo=tmp_path)
        record_bet_placed(100.0, repo=tmp_path)
        assert current_balance(repo=tmp_path) == pytest.approx(900.0)


# ---------------------------------------------------------------------------
# TestRecordBetSettled
# ---------------------------------------------------------------------------


class TestRecordBetSettled:
    """Tests for recording bet settlements."""

    def test_won_credits_return(self, tmp_path: Path) -> None:
        """Won bet: deposit 1000, bet 100, win +150 profit -> balance 1150."""
        deposit(1000.0, repo=tmp_path)
        record_bet_placed(100.0, repo=tmp_path)
        record_bet_settled(100.0, 150.0, repo=tmp_path)  # gross return = 250
        assert current_balance(repo=tmp_path) == pytest.approx(1150.0)

    def test_lost_credits_zero(self, tmp_path: Path) -> None:
        """Lost bet: deposit 1000, bet 100, lose -> balance 900."""
        deposit(1000.0, repo=tmp_path)
        record_bet_placed(100.0, repo=tmp_path)
        record_bet_settled(100.0, -100.0, repo=tmp_path)  # gross return = 0
        assert current_balance(repo=tmp_path) == pytest.approx(900.0)

    def test_push_credits_stake(self, tmp_path: Path) -> None:
        """Push: deposit 1000, bet 100, push -> balance 1000."""
        deposit(1000.0, repo=tmp_path)
        record_bet_placed(100.0, repo=tmp_path)
        record_bet_settled(100.0, 0.0, repo=tmp_path)  # gross return = 100
        assert current_balance(repo=tmp_path) == pytest.approx(1000.0)

    def test_with_reference_id(self, tmp_path: Path) -> None:
        """Settlement stores bet_id as reference_id."""
        record_bet_settled(100.0, 50.0, bet_id="xyz-456", repo=tmp_path)
        df = load_transactions(repo=tmp_path)
        assert df.iloc[0]["reference_id"] == "xyz-456"


# ---------------------------------------------------------------------------
# TestCurrentBalance
# ---------------------------------------------------------------------------


class TestCurrentBalance:
    """Tests for balance calculation."""

    def test_empty(self, tmp_path: Path) -> None:
        """No transactions -> balance is 0."""
        assert current_balance(repo=tmp_path) == 0.0

    def test_deposit_only(self, tmp_path: Path) -> None:
        """Single deposit -> balance equals deposit."""
        deposit(500.0, repo=tmp_path)
        assert current_balance(repo=tmp_path) == pytest.approx(500.0)

    def test_deposit_and_withdraw(self, tmp_path: Path) -> None:
        """Deposit then withdraw -> correct balance."""
        deposit(500.0, repo=tmp_path)
        withdraw(200.0, repo=tmp_path)
        assert current_balance(repo=tmp_path) == pytest.approx(300.0)

    def test_full_cycle(self, tmp_path: Path) -> None:
        """Full cycle: deposit, bet, win -> correct balance."""
        deposit(1000.0, repo=tmp_path)
        record_bet_placed(100.0, repo=tmp_path)
        # Won at +150: pnl = 150, gross return = 250
        record_bet_settled(100.0, 150.0, repo=tmp_path)
        # 1000 - 100 + 250 = 1150
        assert current_balance(repo=tmp_path) == pytest.approx(1150.0)


# ---------------------------------------------------------------------------
# TestBalanceHistory
# ---------------------------------------------------------------------------


class TestBalanceHistory:
    """Tests for balance history."""

    def test_columns(self, tmp_path: Path) -> None:
        """History has the expected columns."""
        deposit(500.0, repo=tmp_path)
        df = balance_history(repo=tmp_path)
        expected: list[str] = [
            "timestamp",
            "txn_type",
            "amount",
            "signed_amount",
            "running_balance",
        ]
        assert list(df.columns) == expected

    def test_running_balance(self, tmp_path: Path) -> None:
        """Running balance accumulates correctly."""
        deposit(500.0, repo=tmp_path)
        withdraw(100.0, repo=tmp_path)
        df = balance_history(repo=tmp_path)
        assert list(df["running_balance"]) == pytest.approx([500.0, 400.0])

    def test_sorted_by_timestamp(self, tmp_path: Path) -> None:
        """History is sorted chronologically."""
        deposit(100.0, repo=tmp_path)
        deposit(200.0, repo=tmp_path)
        withdraw(50.0, repo=tmp_path)
        df = balance_history(repo=tmp_path)
        timestamps: list = list(df["timestamp"])
        assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# TestLoadTransactions
# ---------------------------------------------------------------------------


class TestLoadTransactions:
    """Tests for loading and filtering transactions."""

    def test_load_all(self, tmp_path: Path) -> None:
        """No filter returns all transactions."""
        deposit(100.0, repo=tmp_path)
        withdraw(50.0, repo=tmp_path)
        df = load_transactions(repo=tmp_path)
        assert len(df) == 2

    def test_filter_type(self, tmp_path: Path) -> None:
        """Filtering by txn_type returns only matching rows."""
        deposit(100.0, repo=tmp_path)
        withdraw(50.0, repo=tmp_path)
        deposit(200.0, repo=tmp_path)
        df = load_transactions(txn_type="deposit", repo=tmp_path)
        assert len(df) == 2
        assert all(df["txn_type"] == "deposit")


# ---------------------------------------------------------------------------
# TestSignedAmountSeries (bankroll/H1 - vectorization)
# ---------------------------------------------------------------------------


class TestSignedAmountSeries:
    """Verify the vectorized helper matches the scalar signed_amount semantics."""

    def test_inflows_positive_outflows_negative(self) -> None:
        import pandas as pd

        from gridiron_edge.betting.bankroll import _signed_amount_series

        types = pd.Series(["deposit", "withdraw", "bet_placed", "bet_settled"])
        amounts = pd.Series([100.0, 50.0, 25.0, 75.0])
        signs = _signed_amount_series(types, amounts)
        assert list(signs) == [100.0, -50.0, -25.0, 75.0]

    def test_preserves_index(self) -> None:
        import pandas as pd

        from gridiron_edge.betting.bankroll import _signed_amount_series

        types = pd.Series(["deposit", "withdraw"], index=[5, 9])
        amounts = pd.Series([100.0, 50.0], index=[5, 9])
        signs = _signed_amount_series(types, amounts)
        assert list(signs.index) == [5, 9]

    def test_matches_scalar_signed_amount(self) -> None:
        """Equivalence check: the Series helper produces the same per-row
        result as the scalar signed_amount function."""
        import pandas as pd

        from gridiron_edge.betting.bankroll import (
            _signed_amount_series,
            signed_amount,
        )

        types = pd.Series(["deposit", "withdraw", "bet_placed", "bet_settled"])
        amounts = pd.Series([100.0, 50.0, 25.0, 75.0])

        vectorized = _signed_amount_series(types, amounts)
        scalar = [
            signed_amount(str(txn_type), float(amount))
            for txn_type, amount in zip(types, amounts, strict=True)
        ]

        assert list(vectorized) == scalar


def _write_row(
    repo: Path,
    *,
    timestamp: datetime,
    txn_id: str | None = None,
    txn_type: str = "deposit",
    amount: float = 100.0,
    reference_id: str | None = None,
    note: str | None = None,
) -> None:
    """Append one transaction row with a fully explicit, controlled
    timestamp and (optionally) transaction ID -- bypassing
    deposit()/withdraw()'s internal datetime.now(UTC) and uuid4() calls
    entirely, so tests never depend on wall-clock timing or incidental
    identity differences."""
    existing = bankroll_module._read_txn_log(repo)
    row = pd.DataFrame(
        [
            {
                "txn_id": txn_id or str(uuid_module.uuid4()),
                "timestamp": timestamp,
                "txn_type": txn_type,
                "amount": amount,
                "reference_id": reference_id,
                "note": note,
            }
        ]
    )
    combined = row if existing.empty else pd.concat([existing, row], ignore_index=True)
    bankroll_module._write_txn_log(combined, repo)


class TestBankrollSnapshotAsOf:
    """Tests for cutoff-scoped bankroll evidence derivation.

    Every test writes transaction rows with fully explicit, controlled
    timestamps and transaction IDs via ``_write_row`` -- none depend on
    wall-clock timing, and identity-sensitivity tests use identical fixed
    IDs across compared ledgers so the only true variable is the field
    under test.
    """

    def test_empty_ledger_returns_none(self, tmp_path: Path) -> None:
        """No transactions at all -> no bankroll evidence, not a zero balance."""
        assert bankroll_snapshot_as_of(_T2, repo=tmp_path) is None

    def test_only_post_cutoff_rows_returns_none(self, tmp_path: Path) -> None:
        """Transactions exist, but none are visible by the cutoff -> None."""
        _write_row(tmp_path, timestamp=_T3, amount=500.0)
        assert bankroll_snapshot_as_of(_T2, repo=tmp_path) is None

    def test_visible_transactions_netting_to_zero_returns_real_snapshot(
        self, tmp_path: Path
    ) -> None:
        """A genuine zero balance is a real snapshot, not None."""
        _write_row(tmp_path, timestamp=_T0, txn_type="deposit", amount=500.0)
        _write_row(tmp_path, timestamp=_T1, txn_type="withdraw", amount=500.0)
        snapshot = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        assert snapshot is not None
        assert snapshot.amount == pytest.approx(0.0)

    def test_same_cutoff_same_rows_produce_equal_snapshots(self, tmp_path: Path) -> None:
        """Repeated derivation against an unmodified ledger is stable."""
        _write_row(tmp_path, timestamp=_T0, amount=1000.0)
        first = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        second = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        assert first == second

    def test_post_cutoff_transaction_does_not_change_earlier_snapshot(self, tmp_path: Path) -> None:
        """A transaction recorded after the cutoff has no effect on that cutoff's evidence."""
        _write_row(tmp_path, timestamp=_T0, amount=1000.0)
        before = bankroll_snapshot_as_of(_T1, repo=tmp_path)
        _write_row(tmp_path, timestamp=_T3, amount=500.0)  # strictly after T1
        after = bankroll_snapshot_as_of(_T1, repo=tmp_path)
        assert before == after

    def test_new_pre_cutoff_transaction_changes_amount_and_source_id(self, tmp_path: Path) -> None:
        """A transaction later recorded with a timestamp at/before an
        already-derived cutoff changes both the amount and the identity --
        this is honest behavior, not a reproducibility guarantee against a
        mutable ledger (see module docstring)."""
        _write_row(tmp_path, timestamp=_T0, amount=1000.0)
        first = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        _write_row(tmp_path, timestamp=_T1, amount=250.0)  # strictly before T2
        second = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        assert second.amount != first.amount
        assert second.source_id != first.source_id

    def test_row_order_does_not_affect_source_id(self, tmp_path: Path) -> None:
        """Canonical ordering is deterministic regardless of on-disk row order."""
        _write_row(tmp_path, timestamp=_T0, amount=100.0)
        _write_row(tmp_path, timestamp=_T1, amount=200.0)
        forward = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        df = bankroll_module._read_txn_log(tmp_path)
        reversed_df = df.iloc[::-1].reset_index(drop=True)
        bankroll_module._write_txn_log(reversed_df, tmp_path)
        reversed_snapshot = bankroll_snapshot_as_of(_T2, repo=tmp_path)
        assert forward.source_id == reversed_snapshot.source_id
        assert forward.amount == reversed_snapshot.amount

    def test_naive_cutoff_is_rejected(self, tmp_path: Path) -> None:
        """A naive (non-timezone-aware) cutoff raises ValueError."""
        _write_row(tmp_path, timestamp=_T0, amount=100.0)
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            bankroll_snapshot_as_of(datetime(2026, 9, 1, 12), repo=tmp_path)

    def test_non_utc_cutoff_is_rejected(self, tmp_path: Path) -> None:
        """A non-UTC timezone-aware cutoff raises ValueError."""
        _write_row(tmp_path, timestamp=_T0, amount=100.0)
        non_utc = datetime(2026, 9, 1, 12, tzinfo=timezone(timedelta(hours=-6)))
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            bankroll_snapshot_as_of(non_utc, repo=tmp_path)

    def test_deposit_bet_settle_withdraw_correct_amounts_at_multiple_cutoffs(
        self, tmp_path: Path
    ) -> None:
        """Full lifecycle: amounts are correctly scoped to each distinct cutoff."""
        t_deposit = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
        t_placed = datetime(2026, 9, 1, 11, 0, tzinfo=UTC)
        t_settled = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
        t_withdraw = datetime(2026, 9, 1, 13, 0, tzinfo=UTC)

        _write_row(tmp_path, timestamp=t_deposit, txn_type="deposit", amount=1000.0)
        _write_row(tmp_path, timestamp=t_placed, txn_type="bet_placed", amount=100.0)
        # gross return for a +150 profit win = stake(100) + pnl(150) = 250
        _write_row(tmp_path, timestamp=t_settled, txn_type="bet_settled", amount=250.0)
        _write_row(tmp_path, timestamp=t_withdraw, txn_type="withdraw", amount=200.0)

        after_deposit = t_deposit + timedelta(minutes=1)
        after_placed = t_placed + timedelta(minutes=1)
        after_settled = t_settled + timedelta(minutes=1)
        after_withdraw = t_withdraw + timedelta(minutes=1)

        assert bankroll_snapshot_as_of(after_deposit, repo=tmp_path).amount == pytest.approx(1000.0)
        assert bankroll_snapshot_as_of(after_placed, repo=tmp_path).amount == pytest.approx(900.0)
        assert bankroll_snapshot_as_of(after_settled, repo=tmp_path).amount == pytest.approx(1150.0)
        assert bankroll_snapshot_as_of(after_withdraw, repo=tmp_path).amount == pytest.approx(950.0)

    @pytest.mark.parametrize(
        ("field", "new_value"),
        [
            ("txn_type", "withdraw"),
            ("amount", 999.0),
            ("reference_id", "bet-2"),
        ],
    )
    def test_source_id_reflects_each_material_field_independently(
        self, tmp_path: Path, field: str, new_value: object
    ) -> None:
        """txn_type, amount, and reference_id each independently affect
        source_id when changed. Both ledgers use the identical fixed
        txn_id, and the unmodified evidence is confirmed identical before
        mutating exactly one field -- closing the false-positive gap
        where two randomly-generated txn_ids alone would have made this
        assertion pass regardless of whether the digest covered any
        other field."""
        baseline_repo = tmp_path / "baseline"
        changed_repo = tmp_path / "changed"
        fixed_txn_id = "00000000-0000-0000-0000-000000000001"

        _write_row(
            baseline_repo,
            timestamp=_T0,
            txn_id=fixed_txn_id,
            txn_type="bet_placed",
            amount=100.0,
            reference_id="bet-1",
        )
        _write_row(
            changed_repo,
            timestamp=_T0,
            txn_id=fixed_txn_id,
            txn_type="bet_placed",
            amount=100.0,
            reference_id="bet-1",
        )

        baseline = _snapshot(baseline_repo, _T2)
        unchanged = _snapshot(changed_repo, _T2)
        assert unchanged.source_id == baseline.source_id
        assert unchanged.amount == baseline.amount

        df = bankroll_module._read_txn_log(changed_repo)
        df.loc[0, field] = new_value
        bankroll_module._write_txn_log(df, changed_repo)
        changed = _snapshot(changed_repo, _T2)

        assert changed.source_id != baseline.source_id

    def test_source_id_reflects_txn_id_change(self, tmp_path: Path) -> None:
        """Changing only txn_id (all other fields identical) changes
        source_id -- transaction identity is material evidence, not
        incidental."""
        baseline_repo = tmp_path / "baseline"
        changed_repo = tmp_path / "changed"

        _write_row(
            baseline_repo,
            timestamp=_T0,
            txn_id="00000000-0000-0000-0000-000000000001",
            txn_type="bet_placed",
            amount=100.0,
            reference_id="bet-1",
        )
        _write_row(
            changed_repo,
            timestamp=_T0,
            txn_id="00000000-0000-0000-0000-000000000002",
            txn_type="bet_placed",
            amount=100.0,
            reference_id="bet-1",
        )

        baseline = _snapshot(baseline_repo, _T2)
        changed = _snapshot(changed_repo, _T2)

        assert changed.amount == baseline.amount
        assert changed.source_id != baseline.source_id

    def test_source_id_reflects_timestamp_change(self, tmp_path: Path) -> None:
        """A changed timestamp on a visible row independently affects
        source_id, without moving the row outside the cutoff window and
        without changing the computed amount."""
        _write_row(
            tmp_path,
            timestamp=_T0,
            txn_id="00000000-0000-0000-0000-000000000001",
            txn_type="bet_placed",
            amount=100.0,
            reference_id="bet-1",
        )
        baseline = _snapshot(tmp_path, _T2)

        df = bankroll_module._read_txn_log(tmp_path)
        df.loc[0, "timestamp"] = _T1
        bankroll_module._write_txn_log(df, tmp_path)
        changed = _snapshot(tmp_path, _T2)

        assert changed.amount == baseline.amount
        assert changed.source_id != baseline.source_id

    def test_semantically_equal_timestamps_produce_the_same_identity_across_round_trip(
        self, tmp_path: Path
    ) -> None:
        """A timestamp survives a second Parquet round trip with a stable
        canonical identity contribution -- identity does not depend on
        which in-memory representation (datetime vs. pandas Timestamp)
        happened to be present at digest time."""
        _write_row(
            tmp_path,
            timestamp=_T0,
            txn_id="00000000-0000-0000-0000-000000000001",
            amount=500.0,
        )
        first = _snapshot(tmp_path, _T2)

        df = bankroll_module._read_txn_log(tmp_path)
        bankroll_module._write_txn_log(df, tmp_path)
        second = _snapshot(tmp_path, _T2)

        assert first.source_id == second.source_id
        assert first.amount == second.amount

    def test_source_id_is_pinned_for_one_fixed_transaction_set(self, tmp_path: Path) -> None:
        """Regression-pin the exact source_id for one fixed, fully
        controlled transaction set and cutoff -- proving the digest
        construction itself does not silently drift, not merely that it
        is internally consistent (mirrors WS2 Unit 4's pinned-digest
        precedent). Computed independently via the exact canonical
        payload construction, not guessed."""
        _write_row(
            tmp_path,
            timestamp=_T0,
            txn_id="00000000-0000-0000-0000-000000000001",
            txn_type="deposit",
            amount=1000.0,
            reference_id=None,
        )
        snapshot = _snapshot(tmp_path, _T2)
        assert snapshot.amount == 1000.0
        assert (
            snapshot.source_id == "91a2e24115c82c60c76cb9fdabdeb9198a9265cfddf7c711f0318eca9b56226f"
        )


class TestBankrollEvidenceValidation:
    """Tests for the evidence-boundary validation bankroll_snapshot_as_of enforces."""

    def test_duplicate_txn_id_is_rejected(self, tmp_path: Path) -> None:
        fixed_id = "00000000-0000-0000-0000-000000000001"
        _write_row(tmp_path, timestamp=_T0, txn_id=fixed_id, amount=100.0)
        _write_row(tmp_path, timestamp=_T1, txn_id=fixed_id, amount=200.0)
        with pytest.raises(ValueError, match="duplicate txn_id"):
            bankroll_snapshot_as_of(_T2, repo=tmp_path)

    def test_null_txn_id_is_rejected(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "txn_id": None,
                    "timestamp": _T0,
                    "txn_type": "deposit",
                    "amount": 100.0,
                    "reference_id": None,
                    "note": None,
                }
            ]
        )
        bankroll_module._write_txn_log(df, tmp_path)
        with pytest.raises(ValueError, match="null txn_id"):
            bankroll_snapshot_as_of(_T2, repo=tmp_path)

    def test_naive_transaction_timestamp_is_rejected(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "txn_id": "00000000-0000-0000-0000-000000000001",
                    "timestamp": datetime(2026, 9, 1, 10, 0),
                    "txn_type": "deposit",
                    "amount": 100.0,
                    "reference_id": None,
                    "note": None,
                }
            ]
        )
        bankroll_module._write_txn_log(df, tmp_path)
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            bankroll_snapshot_as_of(_T2, repo=tmp_path)

    def test_unknown_txn_type_is_rejected(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "txn_id": "00000000-0000-0000-0000-000000000001",
                    "timestamp": _T0,
                    "txn_type": "unknown_type",
                    "amount": 100.0,
                    "reference_id": None,
                    "note": None,
                }
            ]
        )
        bankroll_module._write_txn_log(df, tmp_path)
        with pytest.raises(ValueError, match="unknown txn_type"):
            bankroll_snapshot_as_of(_T2, repo=tmp_path)

    def test_negative_amount_is_rejected(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "txn_id": "00000000-0000-0000-0000-000000000001",
                    "timestamp": _T0,
                    "txn_type": "deposit",
                    "amount": -100.0,
                    "reference_id": None,
                    "note": None,
                }
            ]
        )
        bankroll_module._write_txn_log(df, tmp_path)
        with pytest.raises(ValueError, match="non-finite or negative amount"):
            bankroll_snapshot_as_of(_T2, repo=tmp_path)


class TestWriteAtomicity:
    """_write_txn_log publishes atomically; interruption preserves the prior log."""

    def test_temporary_serialization_failure_preserves_existing_log(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        deposit(500.0, repo=tmp_path)
        path = tmp_path / "data" / "betting" / "bankroll_txn.parquet"
        prior_bytes = path.read_bytes()

        real_to_parquet = pd.DataFrame.to_parquet

        def failing_to_parquet(self, target, *a, **k):
            if str(target).endswith(".tmp"):
                raise RuntimeError("simulated temporary serialization failure")
            return real_to_parquet(self, target, *a, **k)

        monkeypatch.setattr(pd.DataFrame, "to_parquet", failing_to_parquet)

        with pytest.raises(RuntimeError, match="simulated temporary serialization failure"):
            deposit(100.0, repo=tmp_path)

        assert path.read_bytes() == prior_bytes
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []

    def test_pre_publication_failure_preserves_existing_log(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        deposit(500.0, repo=tmp_path)
        path = tmp_path / "data" / "betting" / "bankroll_txn.parquet"
        prior_bytes = path.read_bytes()

        def failing_replace(src, dst):
            raise OSError("simulated pre-publication failure")

        monkeypatch.setattr("gridiron_edge.betting.bankroll.os.replace", failing_replace)

        with pytest.raises(OSError, match="simulated pre-publication failure"):
            deposit(100.0, repo=tmp_path)

        assert path.read_bytes() == prior_bytes
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []

    def test_first_write_serialization_failure_leaves_no_log(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "data" / "betting" / "bankroll_txn.parquet"
        assert not path.exists()

        real_to_parquet = pd.DataFrame.to_parquet

        def failing_to_parquet(self, target, *a, **k):
            if str(target).endswith(".tmp"):
                raise RuntimeError("simulated temporary serialization failure")
            return real_to_parquet(self, target, *a, **k)

        monkeypatch.setattr(pd.DataFrame, "to_parquet", failing_to_parquet)

        with pytest.raises(RuntimeError, match="simulated temporary serialization failure"):
            deposit(100.0, repo=tmp_path)

        assert not path.exists()
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []

    def test_successful_write_replaces_destination_atomically(self, tmp_path: Path) -> None:
        deposit(100.0, repo=tmp_path)
        deposit(200.0, repo=tmp_path)
        path = tmp_path / "data" / "betting" / "bankroll_txn.parquet"
        loaded = pd.read_parquet(path)
        assert len(loaded) == 2
        assert list(path.parent.glob(f".{path.name}.*.tmp")) == []
