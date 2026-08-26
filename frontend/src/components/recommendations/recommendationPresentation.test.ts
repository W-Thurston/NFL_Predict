import { describe, expect, it } from "vitest";
import type { components } from "../../api/schema";
import {
  formatSuggestedStake,
  recommendationPresentation,
} from "./recommendationPresentation";

type Recommendation = components["schemas"]["RecommendationPresentation"];

function recommendation(
  resultState: Recommendation["result_state"],
): Recommendation {
  return {
    bankroll_basis: null,
    checks: [],
    decision_quote_age_seconds: 10,
    decision_state: "recommendation_eligible",
    evaluated_at: "2026-09-01T12:10:00Z",
    failed_checks: [],
    forecast_provenance: {
      product_generated_at: "2026-09-01T12:00:00Z",
      product_id: "product-1",
      product_run_id: "run-1",
    },
    issuance_quote_age_seconds: 5,
    offer_provenance: {
      american_price: -110,
      candidate_reference_derivation_version: 1,
      candidate_reference_id: "candidate-1",
      fetched_at: "2026-09-01T12:00:00Z",
      game_id: "2026_01_KC_LAC",
      is_live: false,
      issuance_id: "issuance-1",
      market: "moneyline",
      provider: "the_odds_api",
      side: "home",
    },
    policy_provenance: {
      derivation_method: "method",
      governance_fingerprint: "g",
      policy_id: "policy-1",
      policy_schema_version: 1,
      source_evidence_fingerprint: "e",
    },
    portfolio_observed_at: null,
    portfolio_snapshot_id: null,
    recommendation_eligible: resultState === "recommended",
    result_id: "result-1",
    result_state: resultState,
    sizing: {},
    suggested_stake: resultState === "recommended" ? 12.5 : null,
    supporting_checks: [],
    unavailable_checks: [],
  };
}

describe("recommendationPresentation", () => {
  it.each([
    [null, "Candidate"],
    ["qualified", "Qualified opportunity"],
    ["recommended", "Recommended"],
    ["failed", "Failed qualification"],
    ["unavailable", "Recommendation unavailable"],
    ["conflicting", "Conflicting evidence"],
  ] as const)("maps %s without analytical inference", (state, label) => {
    expect(
      recommendationPresentation(state == null ? null : recommendation(state)).label,
    ).toBe(label);
  });

  it("formats only the persisted suggested stake", () => {
    expect(formatSuggestedStake(recommendation("recommended"))).toBe("$12.50");
    expect(formatSuggestedStake(recommendation("qualified"))).toBeNull();
    expect(formatSuggestedStake(null)).toBeNull();
  });
});
