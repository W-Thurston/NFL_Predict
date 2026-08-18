import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PerformanceChart } from "./PerformanceChart";

const points = [
  { game_date: "2024-09-01", season: "2024-2025", week: 1, value: -1 },
  { game_date: "2025-09-01", season: "2025-2026", week: 1, value: 2 },
];

describe("PerformanceChart", () => {
  it("renders a responsive persisted-series chart", () => {
    const { container } = render(
      <PerformanceChart points={points} valueKind="number" />,
    );
    expect(screen.getByRole("img", { name: "Historical model-performance chart" })).toBeInTheDocument();
    expect(container.querySelector("path")).toBeInTheDocument();
  });

  it("renders an explicit empty state", () => {
    render(<PerformanceChart points={[]} valueKind="percentage" />);
    expect(screen.getByText("No chart data available.")).toBeInTheDocument();
  });
});
