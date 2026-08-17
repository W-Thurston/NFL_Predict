import { describe, expect, it } from "vitest";
import type { GameBetLeg } from "../../utils/betLegs";
import { buildRecordBetRequest } from "./recordWager";

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

describe("buildRecordBetRequest", () => {
  it("uses edited draft terms and null recommendation identities", () => {
    expect(buildRecordBetRequest(leg())).toMatchObject({
      odds: 175,
      stake: 25,
      book: "fanduel",
      recommended_bet_result_id: null,
    });
  });

  it("rejects an incomplete draft", () => {
    expect(buildRecordBetRequest({
      ...leg(),
      draft: { ...leg().draft, proposedStake: null },
    })).toBeNull();
  });
});
