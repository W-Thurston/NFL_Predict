import { describe, expect, it } from "vitest";

import {
  formatCalendarDate,
  formatKickoffDateTime,
} from "./datePresentation";

describe("formatCalendarDate", () => {
  it("formats a date-only value with weekday and no year", () => {
    expect(formatCalendarDate("2026-09-05")).toBe("Sat, Sep 5");
  });

  it.each([null, undefined, "", "2026-02-30", "not-a-date"])(
    "returns null for invalid input %j",
    (value) => {
      expect(formatCalendarDate(value)).toBeNull();
    },
  );
});

describe("formatKickoffDateTime", () => {
  it("formats an instant in Eastern Time", () => {
    expect(
      formatKickoffDateTime("2026-09-10T00:15:00Z", "2026-09-09"),
    ).toBe("WED · SEP 9 · 8:15 PM ET");
  });

  it("formats the date-only fallback when no instant is available", () => {
    expect(formatKickoffDateTime(null, "2026-09-05")).toBe("Sat, Sep 5");
  });

  it("uses the date-only fallback for an invalid instant", () => {
    expect(formatKickoffDateTime("invalid", "2026-09-05")).toBe("Sat, Sep 5");
  });

  it("uses an em dash when neither value is usable", () => {
    expect(formatKickoffDateTime(null, "invalid")).toBe("—");
  });
});
