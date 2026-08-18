import { describe, expect, it } from "vitest";

import {
  rankDifferenceText,
  summarizeRankDifference,
} from "./rankDifference";

describe("summarizeRankDifference", () => {
  it("identifies the offense when its ordinal rank is better", () => {
    const result = summarizeRankDifference(4, 10, "KC", "LAC");
    expect(result).toEqual({
      difference: 6,
      favoredTeam: "KC",
      comparison: "offense",
    });
    expect(rankDifferenceText(result)).toBe("KC ranks 6 places higher");
  });

  it("identifies the defense when its ordinal rank is better", () => {
    const result = summarizeRankDifference(18, 5, "KC", "LAC");
    expect(result).toEqual({
      difference: 13,
      favoredTeam: "LAC",
      comparison: "defense",
    });
    expect(rankDifferenceText(result)).toBe("LAC ranks 13 places higher");
  });

  it("describes only exact equal ranks as equal", () => {
    const result = summarizeRankDifference(7, 7, "KC", "LAC");
    expect(result).toEqual({
      difference: 0,
      favoredTeam: null,
      comparison: "equal",
    });
    expect(rankDifferenceText(result)).toBe("equal ranks");
  });

  it("uses singular place for a one-rank difference", () => {
    const result = summarizeRankDifference(8, 9, "KC", "LAC");
    expect(rankDifferenceText(result)).toBe("KC ranks 1 place higher");
  });
});
