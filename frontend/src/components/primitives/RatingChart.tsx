import { useRef, useState } from "react";

import type { components } from "../../api/schema";
import { Pill } from "./Pill";

type Timeline = components["schemas"]["TeamRatingTimeline"];
type TimelinePoint = components["schemas"]["TeamRatingTimelinePoint"];
type RatingRange = "season" | "recent";

type RatingChartProps = {
  timeline?: Timeline | null;
  range: RatingRange;
  onRangeChange: (range: RatingRange) => void;
  teamName: string;
  height?: number;
  color?: string;
};

type DisplayPoint = {
  key: string;
  label: string;
  season: string;
  week: number | null;
  date: string | null;
  state: TimelinePoint["state"] | "final";
  rating: number | null;
  lowerRating: number | null;
  upperRating: number | null;
  winOutRating: number | null;
  loseOutRating: number | null;
  gamePlayed: boolean;
  result: "W" | "L" | "T" | null;
  opponent: string | null;
};

/** Historical and projected team Elo timeline backed entirely by API evidence. */
export function RatingChart({
  timeline,
  range,
  onRangeChange,
  teamName,
  height = 250,
  color = "var(--pos)",
}: RatingChartProps) {
  const [showScenarios, setShowScenarios] = useState(false);
  const [activePointKey, setActivePointKey] = useState<string | null>(null);
  const [tooltipPosition, setTooltipPosition] = useState<{ left: number; top: number } | null>(null);
  const chartRef = useRef<HTMLDivElement>(null);

  if (!timeline || timeline.points.length === 0) {
    return (
      <div className="dim" style={{ padding: 20, textAlign: "center", fontSize: 12 }}>
        No rating timeline available.
      </div>
    );
  }

  const points = buildDisplayPoints(timeline);
  const width = 960;
  const pad = { top: 24, right: 24, bottom: 42, left: 58 };
  const chartW = width - pad.left - pad.right;
  const chartH = height - pad.top - pad.bottom;
  const values = points.flatMap((point) => [
    point.rating,
    point.lowerRating,
    point.upperRating,
    showScenarios ? point.winOutRating : null,
    showScenarios ? point.loseOutRating : null,
  ]).filter((value): value is number => value != null);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const span = Math.max(rawMax - rawMin, 20);
  const yMin = Math.floor((rawMin - span * 0.08) / 5) * 5;
  const yMax = Math.ceil((rawMax + span * 0.08) / 5) * 5;
  const x = (index: number) =>
    pad.left + (index / Math.max(points.length - 1, 1)) * chartW;
  const y = (value: number) =>
    pad.top + chartH - ((value - yMin) / (yMax - yMin)) * chartH;
  const yTicks = [0, 1, 2, 3, 4].map(
    (index) => yMin + ((yMax - yMin) * index) / 4,
  );
  const historicalPath = pathFor(points, x, y, (point) =>
    point.state === "observed" || point.state === "carried_forward" || point.state === "final"
      ? point.rating
      : null,
  );
  const projectedPath = pathFor(points, x, y, (point) =>
    point.state === "current" || point.state === "forecast" ? point.rating : null,
  );
  const winOutPath = pathFor(points, x, y, (point) =>
    point.state === "current" ? point.rating : point.winOutRating,
  );
  const loseOutPath = pathFor(points, x, y, (point) =>
    point.state === "current" ? point.rating : point.loseOutRating,
  );
  const intervalPath = areaPath(points, x, y);
  const offseasonPath = buildOffseasonPath(points, timeline, x, y);
  const activePoint = points.find((point) => point.key === activePointKey) ?? null;
  const openTooltip = (
    point: DisplayPoint,
    target: SVGCircleElement,
  ) => {
    const chartRect = chartRef.current?.getBoundingClientRect();
    const targetRect = target.getBoundingClientRect();
    if (!chartRect) return;
    setActivePointKey(point.key);
    setTooltipPosition({
      left: targetRect.left + targetRect.width / 2 - chartRect.left,
      top: targetRect.top - chartRect.top,
    });
  };
  const closeTooltip = () => {
    setActivePointKey(null);
    setTooltipPosition(null);
  };

  return (
    <div ref={chartRef} style={{ position: "relative" }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: 12,
          flexWrap: "wrap",
          marginBottom: 10,
        }}
      >
        <div role="group" aria-label="Rating timeline range" style={{ display: "flex", gap: 6 }}>
          <Pill active={range === "season"} onClick={() => onRangeChange("season")}>
            Season
          </Pill>
          <Pill active={range === "recent"} onClick={() => onRangeChange("recent")}>
            Recent
          </Pill>
        </div>
        <Pill active={showScenarios} onClick={() => setShowScenarios((value) => !value)}>
          Show scenarios
        </Pill>
      </div>

      <div className="mono" style={{ display: "flex", gap: 14, flexWrap: "wrap", fontSize: 10, color: "var(--ink-3)" }}>
        <Legend swatch={color}>Historical</Legend>
        <Legend swatch={color} dashed>Projected median</Legend>
        <Legend swatch={color} band>Central 80% interval</Legend>
        {showScenarios && <Legend swatch="var(--pos)" dotted>Win-out scenario</Legend>}
        {showScenarios && <Legend swatch="var(--neg)" dotted>Lose-out scenario</Legend>}
      </div>

      <svg
        aria-label={`${teamName} rating timeline`}
        role="img"
        viewBox={`0 0 ${width} ${height}`}
        style={{ display: "block", width: "100%", height: "auto", marginTop: 4 }}
      >
        {yTicks.map((tick) => (
          <g key={tick}>
            <line x1={pad.left} y1={y(tick)} x2={width - pad.right} y2={y(tick)} stroke="var(--line-soft)" strokeWidth={0.6} />
            <text x={pad.left - 8} y={y(tick)} textAnchor="end" dominantBaseline="central" fill="var(--ink-4)" fontFamily="var(--f-mono)" fontSize={10}>
              {tick.toFixed(0)}
            </text>
          </g>
        ))}
        {intervalPath && <path d={intervalPath} fill={color} opacity={0.13} data-testid="rating-interval" />}
        {historicalPath && <path d={historicalPath} fill="none" stroke={color} strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" data-testid="historical-rating-line" />}
        {offseasonPath && <path d={offseasonPath} fill="none" stroke="var(--ink-3)" strokeWidth={1.5} strokeDasharray="2 5" data-testid="offseason-connector" />}
        {projectedPath && <path d={projectedPath} fill="none" stroke={color} strokeWidth={2} strokeDasharray="7 5" strokeLinecap="round" strokeLinejoin="round" data-testid="projected-rating-line" />}
        {showScenarios && winOutPath && <path d={winOutPath} fill="none" stroke="var(--pos)" strokeWidth={1.35} strokeDasharray="2 4" data-testid="win-out-line" />}
        {showScenarios && loseOutPath && <path d={loseOutPath} fill="none" stroke="var(--neg)" strokeWidth={1.35} strokeDasharray="2 4" data-testid="lose-out-line" />}
        {points.map((point, index) => point.rating == null ? null : (
          <circle
            key={point.key}
            cx={x(index)}
            cy={y(point.rating)}
            r={point.state === "current" ? 4.5 : point.gamePlayed || point.state === "final" ? 2.7 : 1.8}
            fill={point.state === "current" ? color : point.state === "forecast" ? "var(--bg)" : color}
            stroke={color}
            strokeWidth={point.state === "current" ? 2 : 1}
            data-state={point.state}
            tabIndex={0}
            role="button"
            aria-label={tooltipText(point, showScenarios)}
            onMouseEnter={(event) => openTooltip(point, event.currentTarget)}
            onMouseLeave={closeTooltip}
            onFocus={(event) => openTooltip(point, event.currentTarget)}
            onBlur={closeTooltip}
            style={{ cursor: "help", outline: "none" }}
          >
            <title>{tooltipText(point, showScenarios)}</title>
          </circle>
        ))}
        {points.map((point, index) => (
          <text
            key={`axis:${point.key}`}
            x={x(index)}
            y={height - 24}
            textAnchor="middle"
            fill="var(--ink-4)"
            fontFamily="var(--f-mono)"
            fontSize={8}
            data-testid="rating-axis-label"
          >
            <tspan x={x(index)} dy="0">{shortSeason(point.season)}</tspan>
            <tspan x={x(index)} dy="10">{point.week == null ? "Final" : `W${point.week}`}</tspan>
          </text>
        ))}
      </svg>
      {activePoint && tooltipPosition ? (
        <RatingTooltip
          point={activePoint}
          showScenarios={showScenarios}
          left={tooltipPosition.left}
          top={tooltipPosition.top}
        />
      ) : null}

      <div className="mono dim2" style={{ fontSize: 10, marginTop: 4 }}>
        {provenanceText(timeline)}
      </div>
      <div className="mono" style={{ position: "absolute", width: 1, height: 1, overflow: "hidden", clip: "rect(0 0 0 0)" }}>
        {points.map((point) => <span key={`accessible:${point.key}`}>{tooltipText(point, showScenarios)}</span>)}
      </div>
    </div>
  );
}

function buildDisplayPoints(timeline: Timeline): DisplayPoint[] {
  const points: DisplayPoint[] = timeline.points.map((point) => ({
    key: `${point.season}:${point.week}`,
    label: `${shortSeason(point.season)} W${point.week}`,
    season: point.season,
    week: point.week,
    date: point.date ?? null,
    state: point.state,
    rating: point.rating ?? null,
    lowerRating: point.lower_rating ?? null,
    upperRating: point.upper_rating ?? null,
    winOutRating: point.win_out_rating ?? null,
    loseOutRating: point.lose_out_rating ?? null,
    gamePlayed: point.game_played,
    result: point.result ?? null,
    opponent: point.opponent ?? null,
  }));
  const final = timeline.prior_season_final;
  if (!final) return points;
  const insertion = points.findIndex((point) => point.season !== final.season);
  const finalPoint: DisplayPoint = {
    key: `${final.season}:final`,
    label: `${shortSeason(final.season)} Final`,
    season: final.season,
    week: null,
    date: null,
    state: "final",
    rating: final.rating,
    lowerRating: null,
    upperRating: null,
    winOutRating: null,
    loseOutRating: null,
    gamePlayed: final.game_played,
    result: final.result ?? null,
    opponent: final.opponent ?? null,
  };
  if (insertion === -1) return [...points, finalPoint];
  return [...points.slice(0, insertion), finalPoint, ...points.slice(insertion)];
}

function pathFor(
  points: DisplayPoint[],
  x: (index: number) => number,
  y: (value: number) => number,
  valueFor: (point: DisplayPoint) => number | null,
): string {
  let path = "";
  let drawing = false;
  points.forEach((point, index) => {
    const value = valueFor(point);
    if (value == null) {
      drawing = false;
      return;
    }
    path += `${drawing ? " L" : " M"} ${x(index).toFixed(1)},${y(value).toFixed(1)}`;
    drawing = true;
  });
  return path.trim();
}

function areaPath(
  points: DisplayPoint[],
  x: (index: number) => number,
  y: (value: number) => number,
): string {
  const forecast = points
    .map((point, index) => ({ point, index }))
    .filter(({ point }) => point.state === "forecast" && point.lowerRating != null && point.upperRating != null);
  if (forecast.length === 0) return "";
  const upper = forecast.map(({ point, index }) => `${x(index).toFixed(1)},${y(point.upperRating as number).toFixed(1)}`);
  const lower = [...forecast].reverse().map(({ point, index }) => `${x(index).toFixed(1)},${y(point.lowerRating as number).toFixed(1)}`);
  return [`M ${upper[0]}`, ...upper.slice(1).map((value) => `L ${value}`), ...lower.map((value) => `L ${value}`), "Z"].join(" ");
}

function buildOffseasonPath(
  points: DisplayPoint[],
  timeline: Timeline,
  x: (index: number) => number,
  y: (value: number) => number,
): string {
  const transition = timeline.offseason_transition;
  if (!transition) return "";
  const fromIndex = findLastIndex(points, (point) => point.season === transition.from_season && point.rating === transition.from_rating);
  const toIndex = points.findIndex((point) => point.season === transition.to_season && point.week === transition.to_week);
  if (fromIndex === -1 || toIndex === -1) return "";
  return `M ${x(fromIndex).toFixed(1)},${y(transition.from_rating).toFixed(1)} L ${x(toIndex).toFixed(1)},${y(transition.to_rating).toFixed(1)}`;
}

function findLastIndex<T>(items: T[], predicate: (item: T) => boolean): number {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (predicate(items[index])) return index;
  }
  return -1;
}

function tooltipText(point: DisplayPoint, showScenarios: boolean): string {
  const date = point.date ? `${formatDate(point.date)}. ` : "";
  const location = point.state === "final" ? `Final ${point.season} rating` : `${point.season} Week ${point.week}`;
  if (point.rating == null) return `${date}${location}. Rating unavailable.`;
  const value = point.rating.toFixed(0);
  const label = point.state === "forecast" ? "Projected median" : point.state === "current" ? "Current rating" : point.state === "carried_forward" ? "Carried-forward rating" : point.state === "final" ? "Post-Super-Bowl rating" : "Observed rating";
  const parts = [`${date}${location}. ${label}: ${value}.`];
  if (point.lowerRating != null && point.upperRating != null) parts.push(`Central 80% interval: ${point.lowerRating.toFixed(0)}–${point.upperRating.toFixed(0)}.`);
  if (showScenarios && point.winOutRating != null) parts.push(`Win-out scenario: ${point.winOutRating.toFixed(0)}.`);
  if (showScenarios && point.loseOutRating != null) parts.push(`Lose-out scenario: ${point.loseOutRating.toFixed(0)}.`);
  if (point.result && point.opponent) parts.push(`Result: ${point.result} vs ${point.opponent}.`);
  else if (point.state === "carried_forward") parts.push("No game played.");
  return parts.join(" ");
}

function provenanceText(timeline: Timeline): string {
  if (timeline.forecast_simulation_count == null) return "";
  const count = timeline.forecast_simulation_count.toLocaleString("en-US");
  const lower = timeline.forecast_lower_quantile != null ? `P${Math.round(timeline.forecast_lower_quantile * 100)}` : null;
  const upper = timeline.forecast_upper_quantile != null ? `P${Math.round(timeline.forecast_upper_quantile * 100)}` : null;
  const interval = lower && upper ? `${lower}–${upper}` : null;
  return [
    `${count} simulations`,
    interval,
    timeline.forecast_quantile_method ? `${timeline.forecast_quantile_method} quantiles` : null,
  ].filter(Boolean).join(" · ");
}

function shortSeason(season: string): string {
  return `'${season.split("-")[0].slice(-2)}`;
}

function formatDate(value: string): string {
  const date = new Date(`${value}T00:00:00`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

function RatingTooltip({
  point,
  showScenarios,
  left,
  top,
}: {
  point: DisplayPoint;
  showScenarios: boolean;
  left: number;
  top: number;
}) {
  return (
    <div
      role="tooltip"
      style={{
        position: "absolute",
        zIndex: 20,
        left,
        top,
        transform: "translate(-50%, calc(-100% - 10px))",
        minWidth: 160,
        padding: "9px 11px",
        border: "1px solid var(--line)",
        borderRadius: 4,
        backgroundColor: "var(--bg-1)",
        boxShadow: "0 6px 18px rgb(0 0 0 / 35%)",
        pointerEvents: "none",
      }}
    >
      <div style={{ color: "var(--ink)", fontSize: 12, fontWeight: 650 }}>
        {point.date ? formatDate(point.date) : point.state === "final" ? "Season final" : `Week ${point.week}`}
      </div>
      <div className="mono dim" style={{ marginTop: 3, fontSize: 10 }}>
        {point.week == null ? `${shortSeason(point.season)} Final` : `${shortSeason(point.season)} · Week ${point.week}`}
      </div>
      <div className="mono" style={{ marginTop: 4, fontSize: 10, lineHeight: 1.45 }}>
        {point.rating == null ? "Rating unavailable" : `Rating: ${point.rating.toFixed(0)}`}
        {point.lowerRating != null && point.upperRating != null ? (
          <div>P10–P90: {point.lowerRating.toFixed(0)}–{point.upperRating.toFixed(0)}</div>
        ) : null}
        {showScenarios && point.winOutRating != null ? <div>Win out: {point.winOutRating.toFixed(0)}</div> : null}
        {showScenarios && point.loseOutRating != null ? <div>Lose out: {point.loseOutRating.toFixed(0)}</div> : null}
      </div>
    </div>
  );
}

function Legend({ children, swatch, dashed = false, dotted = false, band = false }: { children: React.ReactNode; swatch: string; dashed?: boolean; dotted?: boolean; band?: boolean }) {
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 5 }}>
      <span aria-hidden="true" style={{ width: 18, height: band ? 7 : 2, background: band ? swatch : undefined, opacity: band ? 0.18 : 1, borderTop: band ? undefined : `2px ${dotted ? "dotted" : dashed ? "dashed" : "solid"} ${swatch}` }} />
      {children}
    </span>
  );
}
