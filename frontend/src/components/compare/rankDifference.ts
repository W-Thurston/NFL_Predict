export type RankDifferenceSummary = {
  difference: number;
  favoredTeam: string | null;
  comparison: "equal" | "offense" | "defense";
};

/** Describe an ordinal rank comparison without treating rank distance as effect size. */
export function summarizeRankDifference(
  offenseRank: number,
  defenseRank: number,
  offenseTeam: string,
  defenseTeam: string,
): RankDifferenceSummary {
  const difference = defenseRank - offenseRank;
  if (difference === 0) {
    return { difference: 0, favoredTeam: null, comparison: "equal" };
  }
  return {
    difference: Math.abs(difference),
    favoredTeam: difference > 0 ? offenseTeam : defenseTeam,
    comparison: difference > 0 ? "offense" : "defense",
  };
}

export function rankDifferenceText(summary: RankDifferenceSummary): string {
  if (summary.comparison === "equal") return "equal ranks";
  const places = summary.difference === 1 ? "place" : "places";
  return `${summary.favoredTeam} ranks ${summary.difference} ${places} higher`;
}
