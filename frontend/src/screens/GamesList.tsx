import type { components } from "../api/schema";
import { useLines, useGamesList } from "../api/hooks";
import { useBetSlip } from "../context/BetSlipContext";
import { useAppState } from "../context/AppStateContext";
import { useNav } from "../context/NavContext";
import { TeamMark } from "../components/primitives/TeamMark";
import { Pill } from "../components/primitives/Pill";
import { ErrorCard } from "../components/error/ErrorCard";
import { WeeklyComponentValue } from "../components/field-status/WeeklyComponentValue";
import {
  isWeeklyComponentUsable,
  weeklyComponentStatusMessage,
} from "../components/field-status/weeklyComponentStatus";
import {
  buildGamesCardData,
  type GamesCardData,
  type GamesCardMarket,
  type GamesCardOffer,
} from "../components/games/gameCardData";
import { createGameBetLegFromLineOffer } from "../utils/betLegs";
import {
  normalizeSportsbookKey,
  sportsbookDisplayName,
} from "../utils/sportsbookPreferences";
import {
  formatCalendarDate,
  formatKickoffDateTime,
} from "../utils/datePresentation";

type LineShoppingGame = components["schemas"]["LineShoppingGame"];
type LineOffer = components["schemas"]["LineOffer"];

const MARKETS: Array<{ value: GamesCardMarket; label: string }> = [
  { value: "moneyline", label: "Moneyline" },
  { value: "spread", label: "Spread" },
  { value: "total", label: "Total" },
];
const REGULAR_SEASON_WEEKS = 18;

export function GamesList() {
  const { route, navigate } = useNav();
  const { state } = useAppState();
  const { legs, add } = useBetSlip();
  const market = parseMarket(route.params.market);
  const requestedSeason = normalizeSeason(route.params.season);
  const requestedWeek = normalizeWeek(route.params.week);
  const gamesResult = useGamesList({
    season: requestedSeason ?? undefined,
    week: requestedWeek ?? undefined,
  });
  const resolvedSeason = requestedSeason ?? gamesResult.data?.season ?? null;
  const resolvedWeek = requestedWeek ?? gamesResult.data?.week ?? null;
  const linesResult = useLines({
    season: resolvedSeason ?? undefined,
    week: resolvedWeek ?? undefined,
    market,
  });

  const allSportsbooks = linesResult.data?.sportsbooks ?? [];
  const selected = new Set(state.selectedSportsbooks.map(normalizeSportsbookKey));
  const visibleSportsbooks = allSportsbooks.filter((sportsbook) =>
    state.sportsbookMode === "all"
      ? true
      : selected.has(normalizeSportsbookKey(sportsbook)),
  );
  const lineGames = (linesResult.data?.items ?? []).map((game) => ({
    ...game,
    offers: (game.offers ?? []).filter((offer) =>
      visibleSportsbooks.includes(offer.sportsbook),
    ),
  }));
  const cards = buildGamesCardData({
    games: gamesResult.data?.items ?? [],
    lineGames,
    visibleSportsbooks,
    market,
  }).sort(compareCards);
  const dayGroups = groupCardsByDate(cards);
  const error = gamesResult.error ?? linesResult.error;
  const isLoading = gamesResult.isLoading || linesResult.isLoading;

  const updateScope = (partial: Record<string, string>) => {
    navigate("/games", {
      season: resolvedSeason ?? "",
      week: String(resolvedWeek ?? 1),
      market,
      ...partial,
    });
  };

  return (
    <div className="games-card-screen">
      <header className="hm-card games-card-header">
        <div>
          <div className="upper dim" style={{ fontSize: 10 }}>Games</div>
          <div className="mono dim2" style={{ fontSize: 11, marginTop: 4 }}>
            {resolvedSeason && resolvedWeek
              ? `${resolvedSeason} · Week ${resolvedWeek}`
              : "Schedule and current selected-book markets"}
          </div>
        </div>
        <div className="games-card-controls">
          <div className="games-scope-control" aria-label="Season">
            <button
              type="button"
              aria-label="Previous season"
              disabled={!resolvedSeason}
              onClick={() => resolvedSeason && updateScope({ season: shiftSeason(resolvedSeason, -1) })}
            >
              ‹
            </button>
            <span className="mono">{resolvedSeason ?? "Season"}</span>
            <button
              type="button"
              aria-label="Next season"
              disabled={!resolvedSeason}
              onClick={() => resolvedSeason && updateScope({ season: shiftSeason(resolvedSeason, 1) })}
            >
              ›
            </button>
          </div>
          <label className="games-week-control mono">
            Week
            <select
              aria-label="Week"
              value={resolvedWeek ?? ""}
              onChange={(event) => updateScope({ week: event.target.value })}
            >
              {!resolvedWeek && <option value="">—</option>}
              {Array.from({ length: REGULAR_SEASON_WEEKS }, (_, index) => index + 1).map((week) => (
                <option key={week} value={week}>{week}</option>
              ))}
            </select>
          </label>
          <div className="games-market-controls" aria-label="Market">
            {MARKETS.map((option) => (
              <Pill
                key={option.value}
                active={market === option.value}
                onClick={() => updateScope({ market: option.value })}
              >
                {option.label}
              </Pill>
            ))}
          </div>
        </div>
      </header>

      {isLoading && <div className="hm-card games-card-status">Loading games and markets…</div>}
      {error && (
        <ErrorCard
          error={error}
          onRetry={() => {
            void gamesResult.refetch();
            void linesResult.refetch();
          }}
          title="Couldn't load games"
        />
      )}
      {!isLoading && !error && cards.length === 0 && (
        <div className="hm-card games-card-status">No games found for this week.</div>
      )}
      {!isLoading && !error && dayGroups.map(([date, dayCards]) => (
        <section key={date} className="games-day-section" aria-labelledby={`games-day-${date}`}>
          <div className="games-day-heading">
            <h2 id={`games-day-${date}`}>{formatCalendarDate(date) ?? date}</h2>
            <span className="mono dim2">{dayCards.length} {dayCards.length === 1 ? "game" : "games"}</span>
          </div>
          <div className="games-day-row">
            {dayCards.map((card) => (
              <GameMarketCard
                key={card.game.game_id}
                card={card}
                market={market}
                pickedIds={new Set(legs.map((leg) => leg.id))}
                onView={() => navigate("/games", { gameId: card.game.game_id })}
                onAdd={(lineGame, offer) => add(createGameBetLegFromLineOffer({
                  game: lineGame,
                  offer,
                  source: "games-card",
                  addedAt: new Date().toISOString(),
                }))}
              />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

function GameMarketCard({
  card,
  market,
  pickedIds,
  onView,
  onAdd,
}: {
  card: GamesCardData;
  market: GamesCardMarket;
  pickedIds: Set<string>;
  onView: () => void;
  onAdd: (game: LineShoppingGame, offer: LineOffer) => void;
}) {
  return (
    <article className="hm-card game-market-card">
      <div className="game-market-card__topline mono dim2">
        <span>{formatKickoffDateTime(card.lineGame?.commence_time, card.game.game_date)}</span>
        <button type="button" onClick={onView}>Details →</button>
      </div>
      <div className="game-market-card__matchup" aria-label={`${card.game.away_team} at ${card.game.home_team}`}>
        <span>{card.game.away_team}</span>
        <span className="mono dim2">at</span>
        <span>{card.game.home_team}</span>
      </div>
      <div className="game-comparison-grid">
        <div className="game-comparison-grid__head upper dim">
          <span>Outcome</span>
          <span>Model</span>
          <span>Best offer</span>
        </div>
        {card.offers.map((item) => {
          const legId = item.offer && card.lineGame
            ? createGameBetLegFromLineOffer({
                game: card.lineGame,
                offer: item.offer,
                source: "games-card",
                addedAt: "identity-only",
              }).id
            : null;
          return (
            <ComparisonRow
              key={item.side}
              card={card}
              market={market}
              item={item}
              picked={legId != null && pickedIds.has(legId)}
              onAdd={() => {
                if (card.lineGame && item.offer) onAdd(card.lineGame, item.offer);
              }}
            />
          );
        })}
      </div>
    </article>
  );
}

function ComparisonRow({
  card,
  market,
  item,
  picked,
  onAdd,
}: {
  card: GamesCardData;
  market: GamesCardMarket;
  item: GamesCardOffer;
  picked: boolean;
  onAdd: () => void;
}) {
  return (
    <div className="game-comparison-row">
      <div className="game-comparison-outcome">
        {(item.side === "away" || item.side === "home") && (
          <TeamMark
            abbr={item.side === "away" ? card.game.away_team : card.game.home_team}
            size={26}
          />
        )}
        <span>
          <strong>{outcomeLabel(card, item.side)}</strong>
          <small className="game-comparison-side mono dim2">{sideMarker(item.side)}</small>
        </span>
      </div>
      <div className="game-comparison-model mono tnum">
        <ModelSideValue card={card} market={market} side={item.side} />
      </div>
      <OfferButton item={item} picked={picked} onAdd={onAdd} />
    </div>
  );
}

function OfferButton({
  item,
  picked,
  onAdd,
}: {
  item: GamesCardOffer;
  picked: boolean;
  onAdd: () => void;
}) {
  if (!item.offer) {
    return (
      <div className="game-offer game-offer--unavailable">
        <strong>Unavailable</strong>
      </div>
    );
  }
  return (
    <button
      type="button"
      className={`game-offer${item.recommended ? " game-offer--recommended" : ""}`}
      disabled={picked}
      aria-label={`${picked ? "Already added" : "Add"} ${sideLabel(item.side)} ${formatOffer(item.offer)} at ${sportsbookDisplayName(item.offer.sportsbook)}`}
      onClick={onAdd}
    >
      {item.recommended && <span className="game-offer__recommended">Recommended bet</span>}
      {item.offer.market !== "moneyline" && (
        <strong className="game-offer__line mono tnum">{formatOfferLine(item.offer)}</strong>
      )}
      <strong className="game-offer__price mono tnum">{formatAmericanOdds(item.offer.american_odds)}</strong>
      <span className="game-offer__book mono dim2">{sportsbookDisplayName(item.offer.sportsbook)}</span>
      {picked && <span className="mono dim2">On Bet Slip</span>}
    </button>
  );
}

function ModelSideValue({
  card,
  market,
  side,
}: {
  card: GamesCardData;
  market: GamesCardMarket;
  side: GamesCardOffer["side"];
}) {
  if (market === "moneyline") {
    const value = side === "away"
      ? card.game.win.away_win_prob
      : card.game.win.home_win_prob;
    return (
      <WeeklyComponentValue
        label={`${side === "away" ? "Away" : "Home"} win probability`}
        status={card.game.win.status}
        usable={isWeeklyComponentUsable("win", card.game.win.status)}
        value={value}
        format={(probability) => `${Math.round(probability * 100)}%`}
        statusMessage={weeklyComponentStatusMessage("win", card.game.win.status)}
      />
    );
  }
  if (market === "spread") {
    const homeSpread = card.game.spread.model_spread;
    const value = homeSpread == null
      ? null
      : side === "home"
        ? homeSpread
        : -homeSpread;
    return (
      <WeeklyComponentValue
        label={`${side === "away" ? "Away" : "Home"} spread`}
        status={card.game.spread.status}
        usable={isWeeklyComponentUsable("spread", card.game.spread.status)}
        value={value}
        format={formatSigned}
        statusMessage={weeklyComponentStatusMessage("spread", card.game.spread.status)}
      />
    );
  }
  return (
    <WeeklyComponentValue
      label="Total"
      status={card.game.total.status}
      usable={isWeeklyComponentUsable("total", card.game.total.status)}
      value={card.game.total.model_total}
      format={(value) => value.toFixed(1)}
      statusMessage={weeklyComponentStatusMessage("total", card.game.total.status)}
    />
  );
}

function outcomeLabel(
  card: GamesCardData,
  side: GamesCardOffer["side"],
): string {
  if (side === "away") return teamNickname(card.game.away_team);
  if (side === "home") return teamNickname(card.game.home_team);
  return side === "over" ? "Over" : "Under";
}

function teamNickname(team: string): string {
  const parts = team.trim().split(/\s+/);
  return parts.at(-1) || team;
}

function sideMarker(side: GamesCardOffer["side"]): string {
  if (side === "away") return "Away";
  if (side === "home") return "Home";
  return "Total";
}

function formatOfferLine(offer: LineOffer): string {
  if (offer.line == null) return "—";
  return offer.market === "total"
    ? offer.line.toFixed(1)
    : formatLineValue(offer.line);
}

function formatLineValue(value: number | null | undefined): string {
  if (value == null) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}`;
}

function formatAmericanOdds(value: number): string {
  return value > 0 ? `+${value}` : String(value);
}

function formatOffer(offer: LineOffer): string {
  const odds = formatAmericanOdds(offer.american_odds);
  if (offer.market === "moneyline") return odds;
  return `${formatLineValue(offer.line)} ${odds}`;
}

function sideLabel(side: GamesCardOffer["side"]): string {
  if (side === "over") return "Over";
  if (side === "under") return "Under";
  return side === "away" ? "Away" : "Home";
}

function formatSigned(value: number): string {
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}`;
}

function parseMarket(value: string | undefined): GamesCardMarket {
  return value === "spread" || value === "total" || value === "moneyline"
    ? value
    : "moneyline";
}

function normalizeSeason(value: string | undefined): string | null {
  return value && /^\d{4}-\d{4}$/.test(value) ? value : null;
}

function normalizeWeek(value: string | undefined): number | null {
  const week = Number(value);
  return Number.isInteger(week) && week >= 1 && week <= REGULAR_SEASON_WEEKS ? week : null;
}

function shiftSeason(season: string, delta: number): string {
  const [start] = season.split("-").map(Number);
  return `${start + delta}-${start + delta + 1}`;
}

function compareCards(left: GamesCardData, right: GamesCardData): number {
  const dateOrder = left.game.game_date.localeCompare(right.game.game_date);
  return dateOrder || left.game.game_id.localeCompare(right.game.game_id);
}

function groupCardsByDate(cards: GamesCardData[]): Array<[string, GamesCardData[]]> {
  const groups = new Map<string, GamesCardData[]>();
  for (const card of cards) {
    const items = groups.get(card.game.game_date);
    if (items) items.push(card);
    else groups.set(card.game.game_date, [card]);
  }
  return Array.from(groups.entries());
}
