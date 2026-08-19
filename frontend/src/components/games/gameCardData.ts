import type { components } from "../../api/schema";
import { selectVisibleRecommendedOffer } from "../recommendations/visibleRecommendedOffer";

type GameSummary = components["schemas"]["GameSummary"];
type LineOffer = components["schemas"]["LineOffer"];
type LineShoppingGame = components["schemas"]["LineShoppingGame"];

export type GamesCardMarket = "moneyline" | "spread" | "total";
export type GamesCardSide = LineOffer["side"];

export type GamesCardOffer = {
  side: GamesCardSide;
  offer: LineOffer | null;
  recommended: boolean;
};

export type GamesCardData = {
  game: GameSummary;
  lineGame: LineShoppingGame | null;
  offers: GamesCardOffer[];
};

const SIDES: Record<GamesCardMarket, GamesCardSide[]> = {
  moneyline: ["away", "home"],
  spread: ["away", "home"],
  total: ["over", "under"],
};

/**
 * Join schedule/model data to exact Line Shopping offers by game_id.
 *
 * Every scheduled game remains present. Sportsbook visibility is applied
 * before offer selection. Recommendation styling uses the same shared
 * selector as Line Shopping; no EV or recommendation policy is recreated.
 */
export function buildGamesCardData({
  games,
  lineGames,
  visibleSportsbooks,
  market,
}: {
  games: GameSummary[];
  lineGames: LineShoppingGame[];
  visibleSportsbooks: string[];
  market: GamesCardMarket;
}): GamesCardData[] {
  const lineGamesById = new Map(
    lineGames.map((game) => [game.game_id, game] as const),
  );

  return games.map((game) => {
    const lineGame = lineGamesById.get(game.game_id) ?? null;
    const visibleOffers = (lineGame?.offers ?? []).filter(
      (offer) =>
        offer.market === market &&
        visibleSportsbooks.includes(offer.sportsbook),
    );
    const recommendedOffer = selectVisibleRecommendedOffer(
      visibleOffers,
      visibleSportsbooks,
    );

    return {
      game,
      lineGame,
      offers: SIDES[market].map((side) => {
        const sideOffers = visibleOffers.filter(
          (offer) => offer.side === side,
        );
        const recommendedForSide =
          recommendedOffer?.side === side ? recommendedOffer : null;
        const offer =
          recommendedForSide ?? selectBestVisibleOffer(sideOffers);

        return {
          side,
          offer,
          recommended: offer != null && offer === recommendedOffer,
        };
      }),
    };
  });
}

/** Select a neutral best market offer when this side has no recommendation. */
export function selectBestVisibleOffer(
  offers: LineOffer[],
): LineOffer | null {
  return [...offers].sort(compareVisibleOffers)[0] ?? null;
}

function compareVisibleOffers(left: LineOffer, right: LineOffer): number {
  if (left.is_best_line !== right.is_best_line) {
    return left.is_best_line ? -1 : 1;
  }
  if (left.is_best_price !== right.is_best_price) {
    return left.is_best_price ? -1 : 1;
  }
  if (left.american_odds !== right.american_odds) {
    return right.american_odds - left.american_odds;
  }
  return offerIdentity(left).localeCompare(offerIdentity(right));
}

function offerIdentity(offer: LineOffer): string {
  return [
    offer.provider,
    offer.provider_event_id ?? "",
    offer.sportsbook,
    offer.market,
    offer.side,
    offer.line ?? "",
    offer.american_odds,
    offer.market_fetched_at,
  ].join(":");
}
