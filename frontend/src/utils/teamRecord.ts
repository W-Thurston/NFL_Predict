export type TeamRecordValue = {
  wins?: number | null;
  losses?: number | null;
  ties?: number | null;
};

/** Format one canonical team record, using the caller's unavailable label. */
export function formatTeamRecord(
  record: TeamRecordValue | null | undefined,
  unavailable = "N/A",
): string {
  if (record?.wins == null || record.losses == null) return unavailable;
  if ((record.ties ?? 0) > 0) {
    return `${record.wins}-${record.losses}-${record.ties}`;
  }
  return `${record.wins}-${record.losses}`;
}
