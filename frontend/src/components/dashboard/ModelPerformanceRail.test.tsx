import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ModelPerformanceRail } from "./ModelPerformanceRail";
import { TestWrapper } from "../../test/testWrapper";

vi.mock("../../api/hooks", () => ({
  useHistoricalModelPerformance: vi.fn(),
}));

import { useHistoricalModelPerformance } from "../../api/hooks";

const summary = {
  first_season: "2002-2003",
  last_season: "2025-2026",
  moneyline: {
    evaluated_count: 6483,
    wins: 4134,
    losses: 2349,
    net_wins: 1785,
    accuracy: 0.6377,
  },
  total: {
    decision_count: 6498,
    wins: 3274,
    losses: 3129,
    pushes: 95,
    net_wins: 145,
    hit_rate_excluding_pushes: 0.5113,
  },
};

describe("ModelPerformanceRail", () => {
  it("renders loading state", () => {
    vi.mocked(useHistoricalModelPerformance).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as never);

    render(<TestWrapper><ModelPerformanceRail /></TestWrapper>);
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("renders a compact historical validation snapshot", () => {
    vi.mocked(useHistoricalModelPerformance).mockReturnValue({
      data: summary,
      isLoading: false,
      error: null,
    } as never);

    render(<TestWrapper><ModelPerformanceRail /></TestWrapper>);

    expect(screen.getByText("63.8% accuracy")).toBeInTheDocument();
    expect(screen.getByText("4,134 W · 2,349 L · 6,483 games")).toBeInTheDocument();
    expect(screen.getByText("1,785 more correct than incorrect")).toBeInTheDocument();
    expect(screen.getByText("51.1% hit rate")).toBeInTheDocument();
    expect(screen.getByText("3,274 W · 3,129 L · 95 P · 6,498 decisions")).toBeInTheDocument();
    expect(screen.getByText("145 more wins than losses")).toBeInTheDocument();
    expect(screen.getByText("Historical validation pending")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "View full performance →" })).toBeInTheDocument();
  });

  it("does not render dashboard charts or view controls", () => {
    vi.mocked(useHistoricalModelPerformance).mockReturnValue({
      data: summary,
      isLoading: false,
      error: null,
    } as never);

    const { container } = render(
      <TestWrapper><ModelPerformanceRail /></TestWrapper>,
    );

    expect(container.querySelector("svg")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Net Wins" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Accuracy" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Units" })).not.toBeInTheDocument();
  });
});
