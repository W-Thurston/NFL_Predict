import type { components } from "../../api/schema";

export type LineOffer = components["schemas"]["LineOffer"];

/**
 * Select the visible offer representing the backend's model-selected side.
 */
export function selectVisibleModelSelectedOffer(
  offers: LineOffer[],
  visibleSportsbooks: string[],
): LineOffer | null {
  const modelSelectedSide = offers.find((offer) => offer.is_model_recommended_side)?.side;

  if (!modelSelectedSide) return null;

  const visible = offers
    .filter(
      (offer) =>
        offer.side === modelSelectedSide &&
        visibleSportsbooks.includes(offer.sportsbook) &&
        offer.model_status === "available" &&
        offer.model_probability != null,
    )
    .sort(compareVisibleModelSelectedOffers);

  return visible[0] ?? null;
}

function compareVisibleModelSelectedOffers(left: LineOffer, right: LineOffer): number {
  const probabilityDifference = (right.model_probability ?? 0) - (left.model_probability ?? 0);
  if (probabilityDifference !== 0) return probabilityDifference;

  const lineDifference = compareModelSelectedLines(left, right);
  if (lineDifference !== 0) return lineDifference;

  if (left.american_odds !== right.american_odds) {
    return right.american_odds - left.american_odds;
  }

  return visibleOfferIdentity(left).localeCompare(visibleOfferIdentity(right));
}

function compareModelSelectedLines(left: LineOffer, right: LineOffer): number {
  if (left.line == null || right.line == null || left.line === right.line) return 0;
  if (left.market === "total" && left.side === "over") {
    return left.line - right.line;
  }
  return right.line - left.line;
}

function visibleOfferIdentity(offer: LineOffer): string {
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
