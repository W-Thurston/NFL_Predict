import { describe, expect, it } from "vitest";
import type { components } from "../../api/schema";
import {
  formatAllocationReason,
  formatSuggestedStake,
  hasPositiveAllocation,
  isRecommendationEligible,
  recommendationPresentation,
} from "./recommendationPresentation";

type Recommendation = components["schemas"]["RecommendationPresentation"];

function recommendation(
  resultState: Recommendation["result_state"],
  decisionState: Recommendation["decision_state"] = "recommendation_eligible",
): Recommendation {
  return {
    allocation:
      resultState === "recommended"
        ? { state: "allocated", reason: "allocated", allocated_stake: 12.5 }
        : { state: "not_evaluated", reason: "recommendation_ineligible", allocated_stake: null },
    bankroll_basis: null,
    checks: [],
    decision_quote_age_seconds: 10,
    decision_state: decisionState,
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
    ["conflicting", "Conflicting evidence"],
  ] as const)("maps %s without analytical inference", (state, label) => {
    expect(
      recommendationPresentation(state == null ? null : recommendation(state)).label,
    ).toBe(label);
  });

  it("maps unavailable + insufficient evidence to Insufficient evidence", () => {
    const result = recommendation("unavailable", "insufficient_evidence");
    expect(recommendationPresentation(result).label).toBe("Insufficient evidence");
  });

  it("maps unavailable + recommendation_eligible to allocation-pending, not insufficient evidence", () => {
    const result = recommendation("unavailable", "recommendation_eligible");
    expect(recommendationPresentation(result).label).toBe(
      "Recommended — allocation pending",
    );
  });

  it("formats only the persisted suggested stake", () => {
    expect(formatSuggestedStake(recommendation("recommended"))).toBe("$12.50");
    expect(formatSuggestedStake(recommendation("qualified"))).toBeNull();
    expect(formatSuggestedStake(null)).toBeNull();
  });
});

describe("formatSuggestedStake enforces the allocation contract", () => {
  it("returns the stake only for a positive allocated state", () => {
    const result = recommendation("recommended", "recommendation_eligible");
    expect(formatSuggestedStake(result)).toBe("$12.50");
  });

  it("returns a readable explanation for zero allocation", () => {
    const base = recommendation("recommended", "recommendation_eligible");
    const zeroAllocation: Recommendation = {
      ...base,
      allocation: {
        state: "zero_allocation",
        reason: "correlation_capacity_exhausted",
        allocated_stake: 0,
      },
    };
    expect(formatAllocationReason(zeroAllocation)).toBe("Correlated exposure capacity is exhausted.");
  });

  it("returns null for allocation not evaluated", () => {
    const result = recommendation("unavailable", "recommendation_eligible");
    expect(formatSuggestedStake(result)).toBeNull();
  });

  it("returns null for a null recommendation", () => {
    expect(formatSuggestedStake(null)).toBeNull();
  });
});

describe("formatAllocationReason", () => {
  it("returns null for a positive allocation", () => {
    expect(formatAllocationReason(recommendation("recommended", "recommendation_eligible"))).toBeNull();
  });

  it("returns a readable explanation for zero allocation", () => {
    const result = {
      ...recommendation("recommended", "recommendation_eligible"),
      allocation: { state: "zero_allocation", reason: "correlation_capacity_exhausted", allocated_stake: 0 },
    } as Recommendation;
    expect(formatAllocationReason(result)).toBe("Correlated exposure capacity is exhausted.");
  });

  it("returns null for zero allocation even if suggested_stake were malformed", () => {
    const base = recommendation("recommended", "recommendation_eligible");
    const malformed: Recommendation = {
      ...base,
      allocation: {
        state: "zero_allocation",
        reason: "exact_duplicate_found",
        allocated_stake: 0,
      },
      suggested_stake: 12.5,
    };
    expect(formatSuggestedStake(malformed)).toBeNull();
    expect(formatAllocationReason(malformed)).toBe(
      "An identical position is already on the ledger.",
    );
  });

  it("returns null for a null recommendation", () => {
    expect(formatAllocationReason(null)).toBeNull();
  });
});

describe("isRecommendationEligible / hasPositiveAllocation", () => {
  it("distinguishes eligible-pending, eligible-zero, and eligible-positive from ineligible", () => {
    expect(isRecommendationEligible(recommendation("failed", "unqualified"))).toBe(false);
    expect(isRecommendationEligible(recommendation("unavailable", "recommendation_eligible"))).toBe(true);
    expect(hasPositiveAllocation(recommendation("unavailable", "recommendation_eligible"))).toBe(false);
    expect(hasPositiveAllocation(recommendation("recommended", "recommendation_eligible"))).toBe(true);
  });

  it("returns false for both on a null recommendation", () => {
    expect(isRecommendationEligible(null)).toBe(false);
    expect(hasPositiveAllocation(null)).toBe(false);
  });

  it("distinguishes eligible-pending, eligible-zero, eligible-positive, and ineligible", () => {
    const zeroAllocation: Recommendation = {
      ...recommendation("recommended", "recommendation_eligible"),
      allocation: {
        state: "zero_allocation",
        reason: "correlation_capacity_exhausted",
        allocated_stake: 0,
      },
    };
    expect(isRecommendationEligible(recommendation("failed", "unqualified"))).toBe(false);
    expect(isRecommendationEligible(recommendation("unavailable", "recommendation_eligible"))).toBe(true);
    expect(isRecommendationEligible(zeroAllocation)).toBe(true);
    expect(hasPositiveAllocation(zeroAllocation)).toBe(false);
    expect(hasPositiveAllocation(recommendation("recommended", "recommendation_eligible"))).toBe(true);
  });

});
