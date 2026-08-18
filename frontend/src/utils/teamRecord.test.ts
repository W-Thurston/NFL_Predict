import { describe, expect, it } from "vitest";

import { formatTeamRecord } from "./teamRecord";

describe("formatTeamRecord", () => {
  it("formats wins and losses when ties are zero", () => {
    expect(formatTeamRecord({ wins: 7, losses: 2, ties: 0 })).toBe("7-2");
  });

  it("includes positive ties", () => {
    expect(formatTeamRecord({ wins: 7, losses: 2, ties: 1 })).toBe("7-2-1");
  });

  it("preserves a zero and zero record", () => {
    expect(formatTeamRecord({ wins: 0, losses: 0, ties: 0 })).toBe("0-0");
  });

  it.each([null, undefined, {}, { wins: 1 }, { losses: 1 }])(
    "uses the default unavailable label for %j",
    (record) => {
      expect(formatTeamRecord(record)).toBe("N/A");
    },
  );

  it("supports a compact caller-selected unavailable label", () => {
    expect(formatTeamRecord(null, "—")).toBe("—");
  });
});
