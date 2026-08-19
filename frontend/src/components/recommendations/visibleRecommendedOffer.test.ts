import { describe, expect, it } from "vitest";

import type { LineOffer } from "./visibleRecommendedOffer";
import { selectVisibleRecommendedOffer } from "./visibleRecommendedOffer";

function offer(overrides: Partial<LineOffer> = {}): LineOffer {
  return {
    american_odds: -110,
    is_best_line: false,
    is_best_model_approved_offer: false,
    is_best_price: false,
    is_live: false,
    is_model_approved: false,
    is_model_recommended_offer: false,
    is_model_recommended_side: false,
    market: "spread",
    market_fetched_at: "2026-08-19T12:00:00Z",
    model_probability: 0.55,
    model_status: "available",
    provider: "odds-api",
    side: "home",
    sportsbook: "draftkings",
    ...overrides,
  };
}

describe("selectVisibleRecommendedOffer", () => {
  it("returns null without a model-recommended side", () => {
    expect(selectVisibleRecommendedOffer([offer()], ["draftkings"])).toBeNull();
  });

  it("returns the exact best eligible visible offer without mutating input", () => {
    const hidden = offer({ sportsbook: "fanduel", american_odds: 120, is_model_recommended_side: true });
    const worse = offer({ line: -4.5, is_model_recommended_side: true });
    const expected = offer({ line: -3.5, american_odds: -105, is_model_recommended_side: true });
    const offers = [hidden, worse, expected];
    const original = [...offers];

    const selected = selectVisibleRecommendedOffer(offers, ["draftkings"]);

    expect(selected).toBe(expected);
    expect(offers).toEqual(original);
  });

  it("does not infer visibility from maximum-EV or best-line flags", () => {
    const hidden = offer({
      sportsbook: "fanduel",
      is_model_recommended_side: true,
      is_model_approved: true,
      is_best_line: true,
      is_best_price: true,
      is_best_model_approved_offer: true,
    });
    expect(selectVisibleRecommendedOffer([hidden], ["draftkings"])).toBeNull();
  });

  it("prefers lower totals for Over and higher totals for Under", () => {
    const overHigh = offer({ market: "total", side: "over", line: 46.5, is_model_recommended_side: true });
    const overLow = offer({ market: "total", side: "over", line: 45.5, is_model_recommended_side: true });
    expect(selectVisibleRecommendedOffer([overHigh, overLow], ["draftkings"])).toBe(overLow);

    const underLow = offer({ market: "total", side: "under", line: 45.5, is_model_recommended_side: true });
    const underHigh = offer({ market: "total", side: "under", line: 46.5, is_model_recommended_side: true });
    expect(selectVisibleRecommendedOffer([underLow, underHigh], ["draftkings"])).toBe(underHigh);
  });
});
