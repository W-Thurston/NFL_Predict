/** Keep a city only when the supplied team name does not already contain it. */
export function teamCityForDisplay(
  city: string | null | undefined,
  teamName: string,
): string | undefined {
  const normalizedCity = city?.trim();
  const normalizedName = teamName.trim();
  if (!normalizedCity) return undefined;
  if (
    normalizedName.toLocaleLowerCase().startsWith(
      `${normalizedCity.toLocaleLowerCase()} `,
    )
  ) {
    return undefined;
  }
  return normalizedCity;
}
