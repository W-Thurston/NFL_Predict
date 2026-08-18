const DATE_ONLY = /^(\d{4})-(\d{2})-(\d{2})$/;

function calendarDateParts(value: string): Date | null {
  const match = DATE_ONLY.exec(value);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return date;
}

/** Format a date-only API value without allowing browser timezone shifts. */
export function formatCalendarDate(
  value: string | null | undefined,
): string | null {
  if (!value) return null;
  const date = calendarDateParts(value);
  if (!date) return null;
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    weekday: "short",
    month: "short",
    day: "numeric",
  }).format(date);
}

/** Format an instant in Eastern Time, falling back to a formatted date-only value. */
export function formatKickoffDateTime(
  timestamp: string | null | undefined,
  fallbackDate: string | null | undefined,
): string {
  if (timestamp) {
    const date = new Date(timestamp);
    if (!Number.isNaN(date.getTime())) {
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        weekday: "short",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
      }).formatToParts(date);
      const part = (type: Intl.DateTimeFormatPartTypes) =>
        parts.find((item) => item.type === type)?.value ?? "";
      return [
        part("weekday").toUpperCase(),
        `${part("month").toUpperCase()} ${part("day")}`,
        `${part("hour")}:${part("minute")} ${part("dayPeriod").toUpperCase()} ET`,
      ].join(" · ");
    }
  }
  return formatCalendarDate(fallbackDate) ?? "—";
}
