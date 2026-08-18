import { describe, expect, it } from "vitest";

import { boundedTeamIdentity } from "./teamIdentity";

describe("boundedTeamIdentity", () => {
  it.each([
    ["Kansas City Chiefs", "KCC"],
    ["Los Angeles Chargers", "LAC"],
    ["New York Jets", "NYJ"],
    ["Jaguars", "JAGU"],
    [" KC ", "KC"],
    ["", "?"],
    ["   ", "?"],
  ])("bounds %j to %j", (identity, expected) => {
    expect(boundedTeamIdentity(identity)).toBe(expected);
  });
});
