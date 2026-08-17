import type { components } from "../../api/schema";

export type Recommendation =
  components["schemas"]["RecommendationPresentation"];

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

const PRESENTATIONS = {
  qualified: {
    label: "Qualified opportunity",
    description:
      "The persisted policy evaluation qualified this opportunity, but did not produce a recommendation.",
    tone: "warning",
  },
  recommended: {
    label: "Recommended",
    description:
      "The persisted policy evaluation produced a recommendation with an actionable stake.",
    tone: "positive",
  },
  failed: {
    label: "Failed qualification",
    description:
      "The persisted policy evaluation failed one or more required checks.",
    tone: "negative",
  },
  unavailable: {
    label: "Recommendation unavailable",
    description:
      "The persisted policy evaluation could not determine a recommendation from the available evidence.",
    tone: "unavailable",
  },
  conflicting: {
    label: "Conflicting evidence",
    description:
      "The persisted policy evaluation found conflicting evidence and did not produce a recommendation.",
    tone: "conflicting",
  },
} as const satisfies Record<
  Recommendation["result_state"],
  RecommendationStatePresentation
>;

const CANDIDATE: RecommendationStatePresentation = {
  label: "Candidate",
  description:
    "This exact offer has analytical context but no attached persisted policy result.",
  tone: "candidate",
};

export function recommendationPresentation(
  recommendation: Recommendation | null | undefined,
): RecommendationStatePresentation {
  return recommendation == null
    ? CANDIDATE
    : PRESENTATIONS[recommendation.result_state];
}

export function formatSuggestedStake(
  recommendation: Recommendation | null | undefined,
): string | null {
  const stake = recommendation?.suggested_stake;
  return stake == null ? null : `$${stake.toFixed(2)}`;
}
