/** Return a deterministic compact token while canonical metadata is unavailable. */
export function boundedTeamIdentity(identity: string): string {
  const words = identity.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return "?";
  if (words.length === 1) return words[0].slice(0, 4).toUpperCase();

  const initials = words
    .map((word) => word.match(/[A-Za-z0-9]/)?.[0] ?? "")
    .join("");
  return (initials || words.at(-1)?.slice(0, 3) || "?")
    .slice(0, 4)
    .toUpperCase();
}
