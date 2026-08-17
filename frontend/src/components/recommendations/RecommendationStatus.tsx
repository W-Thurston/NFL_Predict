import type { CSSProperties } from "react";
import {
  recommendationPresentation,
  type Recommendation,
  type RecommendationTone,
} from "./recommendationPresentation";

type RecommendationStatusProps = {
  recommendation: Recommendation | null | undefined;
  compact?: boolean;
};

const COLORS: Record<RecommendationTone, string> = {
  candidate: "var(--ink-3)",
  positive: "var(--pos)",
  warning: "var(--warn)",
  negative: "var(--neg)",
  unavailable: "var(--ink-4)",
  conflicting: "var(--warn)",
};

export function RecommendationStatus({
  recommendation,
  compact = false,
}: RecommendationStatusProps) {
  const presentation = recommendationPresentation(recommendation);
  const style: CSSProperties = {
    display: "inline-flex",
    alignItems: "center",
    border: `1px solid ${COLORS[presentation.tone]}`,
    borderRadius: 999,
    color: COLORS[presentation.tone],
    fontFamily: "var(--f-mono)",
    fontSize: compact ? 8.5 : 9.5,
    lineHeight: 1,
    padding: compact ? "3px 6px" : "4px 8px",
    whiteSpace: "nowrap",
  };

  return (
    <span
      aria-label={`Recommendation state: ${presentation.label}`}
      title={presentation.description}
      style={style}
    >
      {presentation.label}
    </span>
  );
}
