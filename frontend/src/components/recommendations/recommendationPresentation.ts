import type { components } from "../../api/schema";

export type Recommendation =
  components["schemas"]["RecommendationPresentation"];

export type AllocationReason = Recommendation["allocation"]["reason"];

export type RecommendationTone =
  | "candidate"
  | "positive"
  | "warning"
  | "negative"
  | "unavailable"
  | "conflicting";

export type RecommendationStatePresentation = {
  label: string;
  description: string;
  tone: RecommendationTone;
};

const CANDIDATE: RecommendationStatePresentation = {
  label: "Candidate",
  description:
    "This exact offer has analytical context but no attached persisted policy result.",
  tone: "candidate",
};

const QUALIFIED: RecommendationStatePresentation = {
  label: "Qualified opportunity",
  description:
    "The persisted policy evaluation qualified this opportunity, but did not produce a recommendation.",
  tone: "warning",
};

const FAILED: RecommendationStatePresentation = {
  label: "Failed qualification",
  description:
    "The persisted policy evaluation failed one or more required checks.",
  tone: "negative",
};

const CONFLICTING: RecommendationStatePresentation = {
  label: "Conflicting evidence",
  description:
    "The persisted policy evaluation found conflicting evidence and did not produce a recommendation.",
  tone: "conflicting",
};

const INSUFFICIENT_EVIDENCE: RecommendationStatePresentation = {
  label: "Insufficient evidence",
  description:
    "The persisted policy evaluation could not determine a recommendation from the available evidence.",
  tone: "unavailable",
};

const ELIGIBLE_ALLOCATION_PENDING: RecommendationStatePresentation = {
  label: "Recommended — allocation pending",
  description:
    "This is a governed, eligible recommendation, but portfolio allocation could not be evaluated from the available evidence.",
  tone: "unavailable",
};

const RECOMMENDED_ALLOCATED: RecommendationStatePresentation = {
  label: "Recommended",
  description:
    "The persisted policy evaluation produced a recommendation with a positive portfolio allocation.",
  tone: "positive",
};

const RECOMMENDED_ZERO_ALLOCATION: RecommendationStatePresentation = {
  label: "Recommended — zero allocation",
  description:
    "The persisted policy evaluation produced an eligible recommendation, but portfolio policy allocated zero.",
  tone: "warning",
};

export function assertNever(value: never): never {
  throw new Error(`Unsupported recommendation value: ${String(value)}`);
}

/**
 * Present one persisted recommendation result's state, distinguishing
 * recommendation eligibility from portfolio allocation outcome. Never
 * silently relabels an unrecognized result_state -- a new backend state
 * must force this function to be updated, not silently fall through.
 */
export function recommendationPresentation(
  recommendation: Recommendation | null | undefined,
): RecommendationStatePresentation {
  if (recommendation == null) {
    return CANDIDATE;
  }

  switch (recommendation.result_state) {
    case "qualified":
      return QUALIFIED;
    case "failed":
      return FAILED;
    case "conflicting":
      return CONFLICTING;
    case "unavailable":
      return recommendation.decision_state === "recommendation_eligible"
        ? ELIGIBLE_ALLOCATION_PENDING
        : INSUFFICIENT_EVIDENCE;
    case "recommended":
      return recommendation.allocation.state === "zero_allocation"
        ? RECOMMENDED_ZERO_ALLOCATION
        : RECOMMENDED_ALLOCATED;
    default:
      return assertNever(recommendation.result_state);
  }
}

/**
 * True only when the recommendation policy itself established
 * eligibility (Stage 1) -- independent of whether portfolio allocation
 * (Stage 2) is positive, zero, or not yet evaluated. This is NOT the
 * same as "a persisted result is attached" -- a failed, insufficient, or
 * conflicting result is also attached but never eligible.
 */
export function isRecommendationEligible(
  recommendation: Recommendation | null | undefined,
): boolean {
  return recommendation?.decision_state === "recommendation_eligible";
}

/**
 * True only when the recommendation is eligible AND portfolio policy
 * completed a positive allocation -- the only case representing an
 * executable, governed recommendation a user can act on as such. A zero
 * or pending allocation is real, eligible evidence, but it is not this.
 */
export function hasPositiveAllocation(
  recommendation: Recommendation | null | undefined,
): boolean {
  if (recommendation == null) return false;
  return (
    recommendation.decision_state === "recommendation_eligible" &&
    recommendation.allocation.state === "allocated" &&
    recommendation.allocation.allocated_stake != null &&
    recommendation.allocation.allocated_stake > 0
  );
}

/**
 * Format the persisted suggested stake. Enforces the allocation
 * contract itself, rather than trusting the backend to always keep
 * `suggested_stake` null for non-allocated states -- only a completed,
 * positive allocation is ever displayed as a suggested stake.
 */
export function formatSuggestedStake(
  recommendation: Recommendation | null | undefined,
): string | null {
  if (
    recommendation == null ||
    recommendation.allocation.state !== "allocated" ||
    recommendation.suggested_stake == null ||
    recommendation.suggested_stake <= 0
  ) {
    return null;
  }
  return `$${recommendation.suggested_stake.toFixed(2)}`;
}

const ALLOCATION_REASON_LABELS = {
  recommendation_ineligible: "Recommendation is not eligible.",
  allocation_evidence_unavailable: "Allocation evidence is unavailable.",
  allocated: "",
  exact_duplicate_found: "An identical position is already on the ledger.",
  opposing_position_found: "An opposing position is already on the ledger.",
  candidate_capacity_exhausted: "Candidate exposure capacity is exhausted.",
  game_capacity_exhausted: "Game exposure capacity is exhausted.",
  portfolio_capacity_exhausted: "Portfolio exposure capacity is exhausted.",
  correlation_capacity_exhausted: "Correlated exposure capacity is exhausted.",
  below_minimum_actionable_stake:
    "The constrained stake is below the minimum actionable amount.",
} satisfies Record<AllocationReason, string>;

/**
 * Format the machine-readable portfolio-allocation reason for direct
 * display. Returns null for a positive allocation and for a missing
 * recommendation. Never falls back to the raw internal slug for an
 * unrecognized value -- a schema change adding a new reason must be
 * caught here explicitly (via the `satisfies Record<AllocationReason, ...>`
 * exhaustiveness check at compile time), not silently exposed to a user.
 */
export function formatAllocationReason(
  recommendation: Recommendation | null | undefined,
): string | null {
  if (recommendation == null) return null;
  const { allocation } = recommendation;
  if (allocation.state === "allocated") return null;
  return ALLOCATION_REASON_LABELS[allocation.reason] ?? "Unrecognized allocation reason.";
}

/**
 * Shared tone-to-color mapping. Exported so every presentation surface
 * (GameDetail, EdgesTable, BetLegCard) uses one definition instead of
 * three local copies that could silently drift from one another.
 */
export function recommendationToneColor(tone: RecommendationTone): string {
  switch (tone) {
    case "positive":
      return "var(--pos)";
    case "warning":
      return "var(--warn)";
    case "negative":
    case "conflicting":
      return "var(--neg)";
    case "unavailable":
      return "var(--ink-3)";
    case "candidate":
      return "var(--ink-2)";
    default:
      return assertNever(tone);
  }
}
