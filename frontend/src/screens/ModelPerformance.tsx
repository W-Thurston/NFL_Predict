import { useState } from "react";
import {
  useHistoricalModelPerformance,
  useHistoricalModelPerformanceSeries,
} from "../api/hooks";
import { PerformanceChart } from "../components/model-performance/PerformanceChart";
import { ErrorCard } from "../components/error/ErrorCard";

type Market = "moneyline" | "total" | "spread";
type Metric = "net-wins" | "accuracy" | "units";

export function ModelPerformance() {
  const summary = useHistoricalModelPerformance();
  const series = useHistoricalModelPerformanceSeries();
  const [market, setMarket] = useState<Market>("moneyline");
  const [metric, setMetric] = useState<Metric>("net-wins");

  if (summary.isLoading || series.isLoading) {
    return <div className="hm-card" style={{ padding: 24 }}>Loading model performance…</div>;
  }
  if (summary.error) return <ErrorCard error={summary.error} onRetry={() => summary.refetch()} />;
  if (series.error) return <ErrorCard error={series.error} onRetry={() => series.refetch()} />;
  if (!summary.data || !series.data) {
    return <div className="hm-card" style={{ padding: 24 }}>No historical report selected.</div>;
  }

  const report = summary.data;
  const effectiveMetric = market === "moneyline" && metric === "units" ? "net-wins" : metric;
  const chart = chartPoints(market, effectiveMetric, series.data.items);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <section className="hm-card" style={{ padding: 24 }}>
        <div className="upper dim" style={{ fontSize: 10 }}>Model Performance</div>
        <h1 style={{ margin: "8px 0 4px", fontSize: 28 }}>
          Historical walk-forward validation
        </h1>
        <div className="mono dim" style={{ fontSize: 11 }}>
          Current champion algorithms · {report.first_season} through {report.last_season}
        </div>
        <div className="mono dim2" style={{ fontSize: 9, marginTop: 6 }}>
          Report {report.report_id} · selected {formatTimestamp(report.selected_at)}
        </div>
      </section>

      <section className="hm-card" style={{ padding: 20 }}>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          {(["moneyline", "total", "spread"] as Market[]).map((value) => (
            <Toggle key={value} active={market === value} onClick={() => {
              setMarket(value);
              if (value === "spread") setMetric("net-wins");
              if (value === "moneyline" && metric === "units") setMetric("net-wins");
            }}>
              {title(value)}
            </Toggle>
          ))}
        </div>
      </section>

      {market === "spread" ? (
        <SpreadUnavailable />
      ) : (
        <>
          <MetricGrid market={market} report={report} />
          <section className="hm-card" style={{ padding: 24 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
              <div>
                <div className="upper dim" style={{ fontSize: 10 }}>Historical Trend</div>
                <div className="mono dim2" style={{ fontSize: 9, marginTop: 4 }}>
                  Persisted series · no browser-side metric reconstruction
                </div>
              </div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                <Toggle active={effectiveMetric === "net-wins"} onClick={() => setMetric("net-wins")}>Net Wins</Toggle>
                <Toggle active={effectiveMetric === "accuracy"} onClick={() => setMetric("accuracy")}>Accuracy</Toggle>
                {market === "total" && (
                  <Toggle active={effectiveMetric === "units"} onClick={() => setMetric("units")}>Hypothetical Units</Toggle>
                )}
              </div>
            </div>
            <div style={{ marginTop: 18 }}>
              <PerformanceChart
                points={chart}
                valueKind={effectiveMetric === "accuracy" ? "percentage" : effectiveMetric === "units" ? "units" : "number"}
                color={effectiveMetric === "units" ? "var(--neg)" : "var(--pos)"}
              />
            </div>
            <ChartNote market={market} metric={effectiveMetric} report={report} />
          </section>
        </>
      )}

      <Methodology report={report} />
    </div>
  );
}

function MetricGrid({ market, report }: { market: Exclude<Market, "spread">; report: Report }) {
  const metrics = market === "moneyline"
    ? [
        ["Accuracy", percent(report.moneyline.accuracy)],
        ["Record", `${report.moneyline.wins.toLocaleString()} W · ${report.moneyline.losses.toLocaleString()} L`],
        ["Net Correct", signed(report.moneyline.net_wins)],
        ["Brier", fixed(report.moneyline.brier, 4)],
        ["Log Loss", fixed(report.moneyline.log_loss, 4)],
        ["Evaluated Games", report.moneyline.evaluated_count.toLocaleString()],
      ]
    : [
        ["Hit Rate", percent(report.total.hit_rate_excluding_pushes)],
        ["Record", `${report.total.wins.toLocaleString()} W · ${report.total.losses.toLocaleString()} L · ${report.total.pushes.toLocaleString()} P`],
        ["Net Wins", signed(report.total.net_wins)],
        ["MAE", fixed(report.total.mae, 2)],
        ["RMSE", fixed(report.total.rmse, 2)],
        ["Bias", signedFixed(report.total.bias, 2)],
        ["Hypothetical Units", `${signedFixed(report.total.net_units, 2)}u`],
        ["ROI / Unit", percent(report.total.roi_per_unit_staked)],
      ];
  return (
    <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 12 }}>
      {metrics.map(([label, value]) => <MetricCard key={label} label={label} value={value} />)}
    </section>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="hm-card" style={{ padding: 18 }}>
      <div className="upper dim" style={{ fontSize: 9 }}>{label}</div>
      <div className="mono tnum" style={{ fontSize: 22, marginTop: 7 }}>{value}</div>
    </div>
  );
}

function SpreadUnavailable() {
  return (
    <section className="hm-card" style={{ padding: 32 }}>
      <div className="upper dim" style={{ fontSize: 10 }}>Spread</div>
      <h2 style={{ margin: "10px 0 8px" }}>Historical validation is not available</h2>
      <p className="dim" style={{ maxWidth: 720, lineHeight: 1.6 }}>
        Leakage-safe historical Spread calibration is pending. No synthetic results are shown.
      </p>
    </section>
  );
}

function ChartNote({ market, metric, report }: { market: Exclude<Market, "spread">; metric: Metric; report: Report }) {
  let text = "Each correct prediction adds one and each incorrect prediction subtracts one. Pushes contribute zero.";
  if (metric === "accuracy") text = `Trailing ${report.rolling_decision_window} graded decisions.`;
  if (metric === "units") text = report.total.methodology;
  if (market === "moneyline" && metric === "net-wins") text += " Sportsbook pricing is ignored.";
  return <div className="mono dim2" style={{ fontSize: 10, marginTop: 12 }}>{text}</div>;
}

function Methodology({ report }: { report: Report }) {
  return (
    <section className="hm-card" style={{ padding: 24 }}>
      <div className="upper dim" style={{ fontSize: 10, marginBottom: 12 }}>Methodology & Evidence</div>
      <div className="dim" style={{ display: "grid", gap: 8, lineHeight: 1.5, fontSize: 12 }}>
        <div>Season-by-season walk-forward evaluation of the explicitly selected current champion runs.</div>
        <div>Moneyline: {report.moneyline.model_type} · {report.moneyline.run_id}</div>
        <div>Total: {report.total.model_type} · {report.total.run_id}</div>
        <div>Moneyline units are unavailable because historical Moneyline prices were not retained.</div>
        <div>Total units use historical consensus lines, one unit per decision, and assumed -110 pricing.</div>
        <div>Observed historical odds are a future replacement for the assumed-price return layer.</div>
      </div>
    </section>
  );
}

function Toggle({ active, onClick, children }: { active: boolean; onClick: () => void; children: string }) {
  return (
    <button type="button" aria-pressed={active} onClick={onClick} style={{
      padding: "7px 12px", border: "1px solid var(--line-soft)", borderRadius: 4,
      background: active ? "var(--surface-2)" : "transparent", color: active ? "var(--ink-1)" : "var(--ink-3)",
      fontFamily: "var(--f-sans)", fontSize: 10, cursor: "pointer",
    }}>{children}</button>
  );
}

type Report = NonNullable<ReturnType<typeof useHistoricalModelPerformance>["data"]>;
type Point = NonNullable<ReturnType<typeof useHistoricalModelPerformanceSeries>["data"]>["items"][number];

function chartPoints(market: Market, metric: Metric, points: Point[]) {
  return points.map((point) => ({
    game_date: point.game_date,
    season: point.season,
    week: point.week,
    value: market === "moneyline"
      ? metric === "accuracy" ? point.moneyline_rolling_accuracy_100 : point.moneyline_cumulative_net_wins
      : metric === "accuracy" ? point.total_rolling_accuracy_100 : metric === "units" ? point.total_cumulative_units : point.total_cumulative_net_wins,
  }));
}
function title(value: string) { return value.charAt(0).toUpperCase() + value.slice(1); }
function percent(value: number | null) { return value == null ? "—" : `${(value * 100).toFixed(1)}%`; }
function fixed(value: number | null, digits: number) { return value == null ? "—" : value.toFixed(digits); }
function signed(value: number) { return `${value >= 0 ? "+" : ""}${value.toLocaleString()}`; }
function signedFixed(value: number | null, digits: number) { return value == null ? "—" : `${value >= 0 ? "+" : ""}${value.toFixed(digits)}`; }
function formatTimestamp(value: string) { return new Date(value).toLocaleString(); }
