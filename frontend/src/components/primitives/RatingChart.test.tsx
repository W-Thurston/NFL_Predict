import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { components } from "../../api/schema";
import { RatingChart } from "./RatingChart";

type Timeline = components["schemas"]["TeamRatingTimeline"];

function timeline(): Timeline {
  return {
    range: "season",
    completed_through_week: 0,
    current_rating_week: 1,
    points: [
      { season: "2025-2026", week: 22, rating: 1500, state: "carried_forward", game_played: false },
      { season: "2026-2027", week: 1, rating: 1510, state: "current", game_played: false },
      { season: "2026-2027", week: 2, date: "2026-09-20", rating: 1520, state: "forecast", game_played: false, lower_rating: 1500, upper_rating: 1540, win_out_rating: 1545, lose_out_rating: 1495 },
      { season: "2026-2027", week: 18, rating: 1530, state: "forecast", game_played: false, lower_rating: 1480, upper_rating: 1580, win_out_rating: 1650, lose_out_rating: 1400 },
      { season: "2026-2027", week: 19, rating: null, state: "unavailable", game_played: false },
      { season: "2026-2027", week: 22, rating: null, state: "unavailable", game_played: false },
    ],
    prior_season_final: { season: "2025-2026", rating: 1505, source_week: 22, game_played: true, result: "W", opponent: "NE" },
    offseason_transition: { kind: "offseason_adjustment", from_season: "2025-2026", from_rating: 1505, to_season: "2026-2027", to_week: 1, to_rating: 1510 },
    forecast_simulation_count: 10000,
    forecast_lower_quantile: 0.1,
    forecast_center_quantile: 0.5,
    forecast_upper_quantile: 0.9,
    forecast_quantile_method: "linear",
  };
}

describe("RatingChart", () => {
  it("renders historical, current, forecast, interval, final, and provenance semantics", () => {
    render(<RatingChart timeline={timeline()} range="season" onRangeChange={() => undefined} teamName="Miami Dolphins" />);
    expect(screen.getByRole("img", { name: "Miami Dolphins rating timeline" })).toBeInTheDocument();
    expect(screen.getByText("Historical")).toBeInTheDocument();
    expect(screen.getByText("Projected median")).toBeInTheDocument();
    expect(screen.getByText("P10–P90 interval")).toBeInTheDocument();
    expect(screen.getByText(/10,000 simulations · P10–P90 · linear quantiles/)).toBeInTheDocument();
    expect(
      screen.getAllByText(/Final 2025-2026 rating/),
    ).toHaveLength(2);
    expect(screen.queryByText(/Week 23/)).not.toBeInTheDocument();
    expect(screen.getByTestId("rating-interval")).toBeInTheDocument();
    expect(screen.getByTestId("offseason-connector")).toBeInTheDocument();
  });

  it("requests the backend-owned range", () => {
    const onRangeChange = vi.fn();
    render(<RatingChart timeline={timeline()} range="season" onRangeChange={onRangeChange} teamName="Miami Dolphins" />);
    fireEvent.click(screen.getByRole("button", { name: "Recent" }));
    expect(onRangeChange).toHaveBeenCalledWith("recent");
  });

  it("hides scenarios initially and reveals them after the toggle", () => {
    render(<RatingChart timeline={timeline()} range="season" onRangeChange={() => undefined} teamName="Miami Dolphins" />);
    expect(screen.queryByText("Win-out scenario")).not.toBeInTheDocument();
    expect(screen.queryByTestId("win-out-line")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Show scenarios" }));
    expect(screen.getByText("Win-out scenario")).toBeInTheDocument();
    expect(screen.getByText("Lose-out scenario")).toBeInTheDocument();
    expect(screen.getByTestId("win-out-line")).toBeInTheDocument();
    expect(screen.getByTestId("lose-out-line")).toBeInTheDocument();
  });

  it("keeps unsupported playoff weeks unavailable rather than numeric", () => {
    render(<RatingChart timeline={timeline()} range="season" onRangeChange={() => undefined} teamName="Miami Dolphins" />);
    expect(screen.getByText(/2026-2027 Week 19. Rating unavailable/)).toBeInTheDocument();
    expect(screen.getByText(/2026-2027 Week 22. Rating unavailable/)).toBeInTheDocument();
  });

  it("renders an explicit empty state without timeline evidence", () => {
    render(<RatingChart timeline={null} range="season" onRangeChange={() => undefined} teamName="Miami Dolphins" />);
    expect(screen.getByText("No rating timeline available.")).toBeInTheDocument();
  });

  it("shows a visible dated tooltip and labels every axis position with apostrophe years", () => {
    render(<RatingChart timeline={timeline()} range="season" onRangeChange={() => undefined} teamName="Miami Dolphins" />);
    const weekTwo = screen.getByRole("button", { name: /Sep 20, 2026.*Week 2.*Projected median: 1520/ });
    fireEvent.mouseEnter(weekTwo);
    expect(screen.getByRole("tooltip")).toHaveTextContent("Sep 20, 2026");
    expect(screen.getByRole("tooltip")).toHaveTextContent("Week 2");
    expect(screen.getByRole("tooltip")).toHaveTextContent("Rating: 1520");
    expect(screen.getAllByTestId("rating-axis-label")).toHaveLength(timeline().points.length + 1);
    expect(screen.getAllByText("'26").length).toBeGreaterThan(0);
    expect(screen.queryByText(/^26$/)).not.toBeInTheDocument();
  });

  it("uses large interaction targets, a fixed high-contrast palette, and explicit interval boundaries", () => {
    render(
      <RatingChart
        timeline={timeline()}
        range="season"
        onRangeChange={() => undefined}
        teamName="Miami Dolphins"
      />,
    );

    const targets = screen.getAllByTestId("rating-point-hit-target");
    expect(targets.length).toBeGreaterThan(0);
    expect(targets[0]).toHaveAttribute("r", "12");
    expect(screen.getByTestId("historical-rating-line")).toHaveAttribute(
      "stroke",
      "#7dd3fc",
    );
    expect(screen.getByTestId("lower-rating-boundary")).toBeInTheDocument();
    expect(screen.getByTestId("upper-rating-boundary")).toBeInTheDocument();
    expect(screen.getByText(/Median may be asymmetric/)).toBeInTheDocument();

    fireEvent.mouseEnter(targets[0]);
    expect(screen.getByTestId("active-rating-guide")).toBeInTheDocument();
  });

});
