type PerformanceChartPoint = {
  game_date: string;
  season: string;
  week: number;
  value: number | null;
};

type PerformanceChartProps = {
  points: PerformanceChartPoint[];
  valueKind: "number" | "percentage" | "units";
  color?: string;
  height?: number;
};

/** Responsive line chart for persisted historical model-performance series. */
export function PerformanceChart({
  points,
  valueKind,
  color = "var(--pos)",
  height = 280,
}: PerformanceChartProps) {
  const available = points.filter(
    (point): point is PerformanceChartPoint & { value: number } => point.value != null,
  );
  if (available.length === 0) {
    return <div className="dim mono" style={{ padding: 32 }}>No chart data available.</div>;
  }

  const width = 960;
  const pad = { top: 18, right: 24, bottom: 34, left: 64 };
  const values = available.map((point) => point.value);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const includeZero = valueKind !== "percentage";
  const min = includeZero ? Math.min(rawMin, 0) : rawMin;
  const max = includeZero ? Math.max(rawMax, 0) : rawMax;
  const range = max - min || 1;
  const margin = range * 0.08;
  const yMin = min - margin;
  const yMax = max + margin;
  const chartW = width - pad.left - pad.right;
  const chartH = height - pad.top - pad.bottom;
  const x = (index: number) =>
    pad.left + (index / Math.max(available.length - 1, 1)) * chartW;
  const y = (value: number) =>
    pad.top + chartH - ((value - yMin) / (yMax - yMin)) * chartH;
  const path = available
    .map((point, index) =>
      `${index === 0 ? "M" : "L"} ${x(index).toFixed(1)},${y(point.value).toFixed(1)}`,
    )
    .join(" ");
  const yTicks = [0, 1, 2, 3, 4].map(
    (index) => yMin + ((yMax - yMin) * index) / 4,
  );
  const xIndexes = Array.from(
    new Set([0, 1, 2, 3, 4].map((index) =>
      Math.round(((available.length - 1) * index) / 4),
    )),
  );

  return (
    <svg
      aria-label="Historical model-performance chart"
      role="img"
      viewBox={`0 0 ${width} ${height}`}
      style={{ display: "block", width: "100%", height: "auto" }}
    >
      {yTicks.map((tick) => (
        <g key={tick}>
          <line
            x1={pad.left}
            y1={y(tick)}
            x2={width - pad.right}
            y2={y(tick)}
            stroke="var(--line-soft)"
            strokeWidth={0.6}
          />
          <text
            x={pad.left - 8}
            y={y(tick)}
            textAnchor="end"
            dominantBaseline="central"
            fill="var(--ink-4)"
            fontFamily="var(--f-mono)"
            fontSize={10}
          >
            {formatValue(tick, valueKind)}
          </text>
        </g>
      ))}
      {includeZero && yMin <= 0 && yMax >= 0 && (
        <line
          x1={pad.left}
          y1={y(0)}
          x2={width - pad.right}
          y2={y(0)}
          stroke="var(--ink-3)"
          strokeDasharray="5 5"
          strokeWidth={1}
        />
      )}
      <path
        d={path}
        fill="none"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={2}
      />
      {xIndexes.map((index) => {
        const point = available[index];
        return (
          <text
            key={`${point.game_date}-${index}`}
            x={x(index)}
            y={height - 9}
            textAnchor="middle"
            fill="var(--ink-4)"
            fontFamily="var(--f-mono)"
            fontSize={10}
          >
            {point.season}
          </text>
        );
      })}
    </svg>
  );
}

function formatValue(value: number, kind: PerformanceChartProps["valueKind"]) {
  if (kind === "percentage") return `${(value * 100).toFixed(0)}%`;
  if (kind === "units") return `${value.toFixed(0)}u`;
  return value.toFixed(0);
}
