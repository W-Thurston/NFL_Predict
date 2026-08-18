import { useHistoricalModelPerformance } from "../../api/hooks";

/**
 * Compact historical validation snapshot for the dashboard.
 * Detailed series and methodology belong on the dedicated performance page.
 */
export function ModelPerformanceRail() {
  const summary = useHistoricalModelPerformance();

  if (summary.isLoading) return <StateCard message="Loading…" />;
  if (summary.error) return <StateCard message="Couldn't load performance data." />;
  if (!summary.data) {
    return <StateCard message="No historical performance report selected." />;
  }

  const report = summary.data;

  return (
    <div className="hm-card" style={{ padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <div>
          <div className="upper dim" style={{ fontSize: 10 }}>
            Model Performance
          </div>
          <div className="mono dim2" style={{ fontSize: 9, marginTop: 3 }}>
            Walk-forward validation · current champions
          </div>
        </div>
        <div className="mono dim2" style={{ fontSize: 9, textAlign: "right" }}>
          {report.first_season}
          <br />
          through {report.last_season}
        </div>
      </div>

      <div style={{ display: "grid", gap: 12, marginTop: 16 }}>
        <PerformanceBlock
          label="Moneyline"
          headline={`${(report.moneyline.accuracy * 100).toFixed(1)}% accuracy`}
          record={`${report.moneyline.wins.toLocaleString()}-${report.moneyline.losses.toLocaleString()} · ${report.moneyline.evaluated_count.toLocaleString()} games`}
          evidence={`${report.moneyline.net_wins.toLocaleString()} more correct than incorrect`}
        />
        <PerformanceBlock
          label="Total"
          headline={`${(report.total.hit_rate_excluding_pushes * 100).toFixed(1)}% hit rate`}
          record={`${report.total.wins.toLocaleString()}-${report.total.losses.toLocaleString()}-${report.total.pushes.toLocaleString()} · ${report.total.decision_count.toLocaleString()} decisions`}
          evidence={`${report.total.net_wins.toLocaleString()} more wins than losses`}
        />
      </div>

      <div
        style={{
          marginTop: 14,
          paddingTop: 11,
          borderTop: "1px solid var(--line-soft)",
        }}
      >
        <div className="upper dim" style={{ fontSize: 9 }}>
          Spread
        </div>
        <div className="mono dim2" style={{ fontSize: 10, marginTop: 3 }}>
          Historical validation pending
        </div>
      </div>
    </div>
  );
}

function PerformanceBlock({
  label,
  headline,
  record,
  evidence,
}: {
  label: string;
  headline: string;
  record: string;
  evidence: string;
}) {
  return (
    <div
      style={{
        padding: 12,
        border: "1px solid var(--line-soft)",
        borderRadius: 5,
      }}
    >
      <div className="upper dim" style={{ fontSize: 9 }}>
        {label}
      </div>
      <div className="mono tnum" style={{ fontSize: 22, marginTop: 5 }}>
        {headline}
      </div>
      <div className="mono dim" style={{ fontSize: 10, marginTop: 6 }}>
        {record}
      </div>
      <div className="mono dim2" style={{ fontSize: 9, marginTop: 4 }}>
        {evidence}
      </div>
    </div>
  );
}

function StateCard({ message }: { message: string }) {
  return (
    <div className="hm-card" style={{ padding: 20 }}>
      <div className="upper dim" style={{ fontSize: 10, marginBottom: 16 }}>
        Model Performance
      </div>
      <div className="dim mono" style={{ fontSize: 12 }}>
        {message}
      </div>
    </div>
  );
}
