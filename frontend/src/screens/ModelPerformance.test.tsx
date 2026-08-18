import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ModelPerformance } from "./ModelPerformance";
import { TestWrapper } from "../test/testWrapper";

vi.mock("../api/hooks", () => ({
  useHistoricalModelPerformance: vi.fn(),
  useHistoricalModelPerformanceSeries: vi.fn(),
}));
import { useHistoricalModelPerformance, useHistoricalModelPerformanceSeries } from "../api/hooks";

const report = {
  report_id: "r1", selected_at: "2026-08-18T20:20:14Z", first_season: "2002-2003", last_season: "2025-2026", rolling_decision_window: 100,
  moneyline: { model_type: "logistic", run_id: "win-run", evaluated_count: 6483, wins: 4134, losses: 2349, net_wins: 1785, accuracy: 0.6377, brier: 0.2235, log_loss: 0.6377 },
  total: { model_type: "random_forest", run_id: "total-run", decision_count: 6498, wins: 3274, losses: 3129, pushes: 95, net_wins: 145, hit_rate_excluding_pushes: 0.5113, mae: 10.82, rmse: 13.72, bias: -0.75, net_units: -152.64, roi_per_unit_staked: -0.0235, methodology: "Historical consensus-total line; one unit per eligible decision; assumed -110 pricing." },
};
const series = { items: [{ game_date: "2025-09-01", season: "2025-2026", week: 1, moneyline_cumulative_net_wins: 1785, moneyline_rolling_accuracy_100: 0.64, total_cumulative_net_wins: 145, total_rolling_accuracy_100: 0.51, total_cumulative_units: -152.64 }] };
function loaded() {
  vi.mocked(useHistoricalModelPerformance).mockReturnValue({ data: report, isLoading: false, error: null } as never);
  vi.mocked(useHistoricalModelPerformanceSeries).mockReturnValue({ data: series, isLoading: false, error: null } as never);
}

describe("ModelPerformance", () => {
  it("renders Moneyline overview by default", () => {
    loaded(); render(<TestWrapper><ModelPerformance /></TestWrapper>);
    expect(screen.getByText("63.8%")).toBeInTheDocument();
    expect(screen.getByText("4,134-2,349")).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Historical model-performance chart" })).toBeInTheDocument();
  });
  it("switches to Total hypothetical units", () => {
    loaded(); render(<TestWrapper><ModelPerformance /></TestWrapper>);
    fireEvent.click(screen.getByRole("button", { name: "Total" }));
    fireEvent.click(screen.getByRole("button", { name: "Hypothetical Units" }));
    expect(screen.getAllByText("-152.64u").length).toBeGreaterThan(0);
    expect(screen.getAllByText(/assumed -110 pricing/)).toHaveLength(2);
  });
  it("shows an explicit Spread unavailable state", () => {
    loaded(); render(<TestWrapper><ModelPerformance /></TestWrapper>);
    fireEvent.click(screen.getByRole("button", { name: "Spread" }));
    expect(screen.getByText("Historical validation is not available")).toBeInTheDocument();
    expect(screen.getByText(/No synthetic results/)).toBeInTheDocument();
  });
});
