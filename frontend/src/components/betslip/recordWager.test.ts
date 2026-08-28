import { describe, expect, it } from "vitest";

import type { GameBetLeg } from "../../utils/betLegs";
import { buildRecordBetRequest } from "./recordWager";

type PersistedRecommendation =
  NonNullable<GameBetLeg["persistedRecommendation"]>;

function leg(): GameBetLeg {
  return {
    version: 4,
    kind: "game",
    id: "game:2026_01_KC_LAC:moneyline:away:none:fanduel",
    source: "betslip-edges",
    addedAt: "2026-09-01T12:00:00Z",
    gameId: "2026_01_KC_LAC",
    awayTeam: "KC",
    homeTeam: "LAC",
    market: "moneyline",
    side: "away",
    line: null,
    persistedRecommendation: null,
    edgeAnalytics: {
      modelKey: "win_prob_random_forest",
      referenceAmericanOdds: 170,
      referenceModelProbability: 0.42,
      referenceModelValue: 0.42,
      referenceMarketValue: null,
      referenceExpectedValue: 0.08,
      referenceEdgeStrength: "strong",
      referenceProvider: "the_odds_api",
      referenceProviderEventId: "event-1",
      referenceSportsbook: "draftkings",
      referenceMarketFetchedAt: "2026-09-01T12:00:00Z",
      referenceSportsbookUpdatedAt: null,
      referenceCommenceTime: null,
      referenceIsLive: false,
    },
    draft: {
      currentAmericanOdds: 175,
      proposedStake: 25,
      sportsbook: "fanduel",
      note: null,
    },
  };
}

function persistedRecommendation(): PersistedRecommendation {
  return {
    result_id: "result-1",
    evaluation_id: "evaluation-1",
    decision_state: "recommendation_eligible",
    result_state: "recommended",
    recommendation_eligible: true,
    evaluated_at: "2026-09-01T12:10:00Z",
    issuance_quote_age_seconds: 5,
    decision_quote_age_seconds: 10,
    checks: [],
    supporting_checks: [],
    failed_checks: [],
    unavailable_checks: [],
    sizing: {},
    allocation: {
      state: "allocated",
      reason: "allocated",
      allocated_stake: 25,
    },
    suggested_stake: 25,
    bankroll_basis: null,
    portfolio_snapshot_id: null,
    portfolio_observed_at: null,
    offer_provenance: {
      issuance_id: "issuance-1",
      candidate_reference_id: "candidate-1",
      candidate_reference_derivation_version: 1,
      game_id: "2026_01_KC_LAC",
      market: "moneyline",
      side: "away",
      provider: "the_odds_api",
      provider_event_id: "event-1",
      sportsbook: "draftkings",
      fetched_at: "2026-09-01T12:00:00Z",
      sportsbook_updated_at: null,
      kickoff: null,
      is_live: false,
      line: null,
      american_price: 170,
    },
    forecast_provenance: {
      product_id: "product-1",
      product_run_id: "run-1",
      product_generated_at: "2026-09-01T12:00:00Z",
    },
    policy_provenance: {
      policy_id: "policy-1",
      policy_schema_version: 1,
      source_evidence_fingerprint: "evidence-1",
      governance_fingerprint: "governance-1",
      derivation_method: "method",
    },
  };
}

function recommendationBackedLeg(
  overrides: Partial<PersistedRecommendation> = {},
): GameBetLeg {
  const persisted = persistedRecommendation();

  return {
    ...leg(),
    persistedRecommendation: {
      ...persisted,
      ...overrides,
      offer_provenance: {
        ...persisted.offer_provenance,
        ...overrides.offer_provenance,
      },
      policy_provenance: {
        ...persisted.policy_provenance,
        ...overrides.policy_provenance,
      },
    },
  };
}

const NULL_RECOMMENDATION_IDENTITIES = {
  recommended_bet_result_id: null,
  recommendation_evaluation_id: null,
  candidate_reference_id: null,
  recommendation_policy_id: null,
};

describe("buildRecordBetRequest", () => {
  it("uses edited draft terms and null recommendation identities", () => {
    expect(buildRecordBetRequest(leg())).toMatchObject({
      odds: 175,
      stake: 25,
      book: "fanduel",
      ...NULL_RECOMMENDATION_IDENTITIES,
    });
  });

  it("rejects an incomplete draft", () => {
    expect(
      buildRecordBetRequest({
        ...leg(),
        draft: {
          ...leg().draft,
          proposedStake: null,
        },
      }),
    ).toBeNull();
  });

  it("emits the complete persisted recommendation identity chain", () => {
    expect(
      buildRecordBetRequest(
        recommendationBackedLeg(),
      ),
    ).toMatchObject({
      recommended_bet_result_id: "result-1",
      recommendation_evaluation_id: "evaluation-1",
      candidate_reference_id: "candidate-1",
      recommendation_policy_id: "policy-1",
    });
  });

  it.each([
    [
      "empty result identity",
      recommendationBackedLeg({
        result_id: "",
      }),
    ],
    [
      "whitespace result identity",
      recommendationBackedLeg({
        result_id: "   ",
      }),
    ],
    [
      "empty evaluation identity",
      recommendationBackedLeg({
        evaluation_id: "",
      }),
    ],
    [
      "whitespace evaluation identity",
      recommendationBackedLeg({
        evaluation_id: "   ",
      }),
    ],
  ])(
    "emits no recommendation identities for %s",
    (_label, candidateLeg) => {
      expect(
        buildRecordBetRequest(candidateLeg),
      ).toMatchObject(
        NULL_RECOMMENDATION_IDENTITIES,
      );
    },
  );

  it("emits no recommendation identities for an empty candidate reference", () => {
    const candidateLeg =
      recommendationBackedLeg();

    candidateLeg.persistedRecommendation = {
      ...candidateLeg.persistedRecommendation!,
      offer_provenance: {
        ...candidateLeg.persistedRecommendation!
          .offer_provenance,
        candidate_reference_id: "",
      },
    };

    expect(
      buildRecordBetRequest(candidateLeg),
    ).toMatchObject(
      NULL_RECOMMENDATION_IDENTITIES,
    );
  });

  it("emits no recommendation identities for an empty policy identity", () => {
    const candidateLeg =
      recommendationBackedLeg();

    candidateLeg.persistedRecommendation = {
      ...candidateLeg.persistedRecommendation!,
      policy_provenance: {
        ...candidateLeg.persistedRecommendation!
          .policy_provenance,
        policy_id: "",
      },
    };

    expect(
      buildRecordBetRequest(candidateLeg),
    ).toMatchObject(
      NULL_RECOMMENDATION_IDENTITIES,
    );
  });
});
