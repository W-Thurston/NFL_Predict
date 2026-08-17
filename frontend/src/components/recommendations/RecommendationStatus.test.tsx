import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { RecommendationStatus } from "./RecommendationStatus";

 describe("RecommendationStatus", () => {
  it("renders candidate when no persisted result is attached", () => {
    render(<RecommendationStatus recommendation={null} />);
    expect(screen.getByText("Candidate")).toHaveAccessibleName(
      "Recommendation state: Candidate",
    );
  });
});
