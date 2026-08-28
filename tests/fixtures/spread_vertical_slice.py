"""Deterministic game-spread evidence for vertical-slice integration proofs."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

from pandas import DataFrame

from gridiron_edge.evaluation.forecast_contracts import WeeklyProductIdentity
from gridiron_edge.evaluation.forecast_store import FORECAST_EVENT_COLUMNS
from gridiron_edge.ingest.odds.store import QUOTE_COLUMNS
from gridiron_edge.market.recommendation_policy import (
    RECOMMENDATION_POLICY_DERIVATION_METHOD,
    RECOMMENDATION_POLICY_SCHEMA_VERSION,
    BankrollBasis,
    EmpiricalQualificationThresholds,
    MarketFamilyRecommendationPolicy,
    PolicyDerivationReason,
    PolicyDerivationStatus,
    PolicyValueSource,
    PortfolioExposureSnapshot,
    RecommendationPolicy,
    RecommendationPolicyGovernance,
    StakeRoundingMode,
    governance_fingerprint,
    portfolio_exposure_snapshot,
    recommendation_policy_id,
)

GAME_ID = "2026_01_KC_LAC"
SEASON = "2026-2027"
WEEK = 1
PRODUCT_ID = "spread-vertical-slice-product"
PRODUCT_RUN_ID = "spread-vertical-slice-run"
FORECAST_EVENT_ID = "spread-vertical-slice-forecast"
TOTAL_EVENT_ID = "spread-vertical-slice-total"

PRODUCT_GENERATED_AT = datetime(2026, 9, 1, 11, tzinfo=UTC)
T1 = datetime(2026, 9, 1, 12, tzinfo=UTC)
T2 = datetime(2026, 9, 2, 9, tzinfo=UTC)
T1_FETCHED_AT = datetime(2026, 9, 1, 11, 30, tzinfo=UTC)
T1_UPDATED_AT = datetime(2026, 9, 1, 11, 29, tzinfo=UTC)
T2_FETCHED_AT = datetime(2026, 9, 2, 8, 30, tzinfo=UTC)
T2_UPDATED_AT = datetime(2026, 9, 2, 8, 29, tzinfo=UTC)
KICKOFF = datetime(2026, 9, 10, 0, 20, tzinfo=UTC)


def weekly_product_identity() -> WeeklyProductIdentity:
    """Return the immutable identity for the selected weekly product."""
    return WeeklyProductIdentity(
        product_id=PRODUCT_ID,
        run_id=PRODUCT_RUN_ID,
        season=SEASON,
        week=WEEK,
        generated_at=PRODUCT_GENERATED_AT,
    )


def weekly_product() -> DataFrame:
    """Return one validated unstamped weekly-product row."""
    return DataFrame(
        [
            {
                "season": SEASON,
                "week": WEEK,
                "game_id": GAME_ID,
                "away_team": "Kansas City Chiefs",
                "home_team": "Los Angeles Chargers",
                "neutral_site": False,
                "win_status": "available",
                "win_selection_status": "selected",
                "away_win_prob": 0.40,
                "home_win_prob": 0.60,
                "win_model_name": "win_prob",
                "win_model_type": "elo",
                "win_event_id": FORECAST_EVENT_ID,
                "win_run_id": PRODUCT_RUN_ID,
                "win_generated_at": PRODUCT_GENERATED_AT,
                "win_role": "live",
                "spread_status": "available",
                "model_spread": -3.0,
                "spread_uncertainty": 13.5,
                "spread_source_event_id": FORECAST_EVENT_ID,
                "spread_model_name": "win_prob",
                "spread_model_type": "elo",
                "spread_calibration_key": "win_prob_elo",
                "spread_calibration_updated_at": "2026-07-30T12:00:00+00:00",
                "total_status": "available",
                "model_total": 44.0,
                "total_uncertainty": 12.8,
                "total_model_name": "total",
                "total_model_type": "xgboost",
                "total_event_id": TOTAL_EVENT_ID,
                "total_run_id": PRODUCT_RUN_ID,
                "total_generated_at": PRODUCT_GENERATED_AT,
                "total_role": "live",
                "total_selection_status": "selected",
                "total_uncertainty_trained_at": "2026-07-01T14:20:00",
                "projected_score_status": "available",
                "projected_home_score": 23.5,
                "projected_away_score": 20.5,
            }
        ]
    )


def _forecast_event(
    *,
    event_id: str,
    model_name: str,
    model_type: str,
    home_win_prob: float | None = None,
    model_total: float | None = None,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "run_id": PRODUCT_RUN_ID,
        "role": "live",
        "generated_at": PRODUCT_GENERATED_AT,
        "season": SEASON,
        "week": WEEK,
        "game_id": GAME_ID,
        "model_name": model_name,
        "model_type": model_type,
        "game_date": "2026-09-10",
        "away_team": "Kansas City Chiefs",
        "home_team": "Los Angeles Chargers",
        "away_elo": None,
        "home_elo": None,
        "away_win_prob": None if home_win_prob is None else 1.0 - home_win_prob,
        "home_win_prob": home_win_prob,
        "model_spread": None,
        "model_total": model_total,
        "projected_home_score": None,
        "projected_away_score": None,
        "margin_std": None,
        "win_prob_lo": 0.47 if home_win_prob is not None else None,
        "win_prob_hi": 0.73 if home_win_prob is not None else None,
        "confidence_tier": "Low" if home_win_prob is not None else None,
    }


def forecast_events() -> DataFrame:
    """Return the exact forecast events referenced by the product."""
    return DataFrame(
        [
            _forecast_event(
                event_id=FORECAST_EVENT_ID,
                model_name="win_prob",
                model_type="elo",
                home_win_prob=0.60,
            ),
            _forecast_event(
                event_id=TOTAL_EVENT_ID,
                model_name="total",
                model_type="xgboost",
                model_total=44.0,
            ),
        ],
        columns=FORECAST_EVENT_COLUMNS,
    )


def spread_quote(
    *,
    fetched_at: datetime,
    sportsbook_updated_at: datetime,
    line: float,
    odds: float = -110.0,
) -> dict[str, object]:
    """Return one canonical exact spread observation."""
    return {
        "fetched_at": fetched_at,
        "provider": "the_odds_api",
        "provider_event_id": "provider-event-1",
        "sportsbook": "draftkings",
        "sportsbook_updated_at": sportsbook_updated_at,
        "commence_time": KICKOFF,
        "is_live": False,
        "season": SEASON,
        "week": WEEK,
        "game_id": GAME_ID,
        "game_date": "2026-09-10",
        "away_team": "Kansas City Chiefs",
        "home_team": "Los Angeles Chargers",
        "market": "spread",
        "side": "home",
        "odds": odds,
        "line": line,
    }


def t1_quotes() -> DataFrame:
    """Return the original positive-EV source observation."""
    return DataFrame(
        [
            spread_quote(
                fetched_at=T1_FETCHED_AT,
                sportsbook_updated_at=T1_UPDATED_AT,
                line=-1.0,
            )
        ],
        columns=list(QUOTE_COLUMNS),
    )


def t2_quotes() -> DataFrame:
    """Return the later negative-EV source observation."""
    return DataFrame(
        [
            spread_quote(
                fetched_at=T2_FETCHED_AT,
                sportsbook_updated_at=T2_UPDATED_AT,
                line=-9.5,
            )
        ],
        columns=list(QUOTE_COLUMNS),
    )


def active_governance() -> RecommendationPolicyGovernance:
    """Return deterministic governance supporting recommendation and allocation."""
    return RecommendationPolicyGovernance(
        fractional_kelly_multiplier=0.25,
        minimum_actionable_stake=5.0,
        stake_increment=1.0,
        stake_rounding=StakeRoundingMode.DOWN,
        maximum_candidate_bankroll_fraction=0.02,
        maximum_game_bankroll_fraction=0.05,
        maximum_portfolio_bankroll_fraction=0.20,
        prohibit_opposing_positions=True,
        correlation_check_mandatory=False,
        exposure_eligible_statuses=("open",),
    )


def _family(
    market: str,
    *,
    active: bool,
) -> MarketFamilyRecommendationPolicy:
    return MarketFamilyRecommendationPolicy(
        market=market,
        status=(
            PolicyDerivationStatus.ACTIVE
            if active
            else PolicyDerivationStatus.INSUFFICIENT_EVIDENCE
        ),
        reason=(
            PolicyDerivationReason.DERIVED
            if active
            else PolicyDerivationReason.NO_VALIDATED_THRESHOLD_SELECTION_METHOD
        ),
        candidate_count=2,
        outcome_available_count=2,
        clv_available_count=2,
        return_available_count=2,
        evidence_statuses=(("controlled_real_store_evidence", "available"),),
        thresholds=(
            EmpiricalQualificationThresholds(
                minimum_expected_value=0.01,
                maximum_quote_age_seconds=3600.0,
                minimum_observation_count=None,
                minimum_distinct_fetch_count=None,
            )
            if active
            else None
        ),
        source=PolicyValueSource.EMPIRICAL_MARKET_FAMILY_EVIDENCE,
    )


def recommendation_policy(
    *,
    spread_active: bool,
) -> RecommendationPolicy:
    """Return an internally valid active or abstaining spread policy."""
    governed = active_governance()
    policy = RecommendationPolicy(
        schema_version=RECOMMENDATION_POLICY_SCHEMA_VERSION,
        policy_id="0" * 64,
        created_at=datetime(2026, 8, 17, tzinfo=UTC),
        source_evidence_fingerprint="a" * 64,
        governance_fingerprint=governance_fingerprint(governed),
        derivation_method=RECOMMENDATION_POLICY_DERIVATION_METHOD,
        moneyline=_family("moneyline", active=False),
        spread=_family("spread", active=spread_active),
        total=_family("total", active=False),
        governance=governed,
    )
    return replace(
        policy,
        policy_id=recommendation_policy_id(policy),
    )


def bankroll() -> BankrollBasis:
    """Return reproducible decision-time bankroll evidence."""
    return BankrollBasis(
        amount=1000.0,
        observed_at=T1,
        source_kind="transaction_snapshot",
        source_id="spread-proof-bankroll",
    )


def empty_portfolio() -> PortfolioExposureSnapshot:
    """Return complete portfolio evidence with no existing exposure."""
    return portfolio_exposure_snapshot(
        observed_at=T1,
        rows=(),
    )
