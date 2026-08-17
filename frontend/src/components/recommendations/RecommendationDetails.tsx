import type { components } from "../../api/schema";
import {
  formatSuggestedStake,
  type Recommendation,
} from "./recommendationPresentation";
import { RecommendationStatus } from "./RecommendationStatus";

type RecommendationCheck =
  components["schemas"]["RecommendationCheckResponse"];

type RecommendationDetailsProps = {
  recommendation: Recommendation;
  summary?: string;
};

function CheckList({
  title,
  checks,
}: {
  title: string;
  checks: RecommendationCheck[];
}) {
  if (checks.length === 0) return null;
  return (
    <section aria-label={title}>
      <strong>{title}</strong>
      <ul>
        {checks.map((check) => (
          <li key={check.check_id}>
            <span>{check.check_id}</span>
            {" · "}
            <span>{check.reason}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function RecommendationDetails({
  recommendation,
  summary = "Recommendation evidence",
}: RecommendationDetailsProps) {
  const stake = formatSuggestedStake(recommendation);
  const policy = recommendation.policy_provenance;
  const offer = recommendation.offer_provenance;
  const forecast = recommendation.forecast_provenance;

  return (
    <details>
      <summary>{summary}</summary>
      <div
        className="mono"
        style={{ display: "grid", gap: 10, paddingTop: 10, fontSize: 10 }}
      >
        <RecommendationStatus recommendation={recommendation} />
        <dl style={{ display: "grid", gap: 4, margin: 0 }}>
          <div>
            <dt>Persisted suggested stake</dt>
            <dd>{stake ?? "Unavailable"}</dd>
          </div>
          <div>
            <dt>Policy</dt>
            <dd>
              Schema {policy.policy_schema_version} · {policy.policy_id}
            </dd>
          </div>
          <div>
            <dt>Evaluated</dt>
            <dd>{recommendation.evaluated_at}</dd>
          </div>
          <div>
            <dt>Exact offer</dt>
            <dd>
              {offer.provider} · {offer.sportsbook ?? "Consensus"} · {offer.game_id}
              {" · "}{offer.market} {offer.side}
            </dd>
          </div>
          <div>
            <dt>Forecast</dt>
            <dd>
              {forecast.model_name ?? "Unavailable"} · {forecast.model_type ?? "Unavailable"}
              {" · "}{forecast.product_id}
            </dd>
          </div>
        </dl>
        <CheckList title="Supporting checks" checks={recommendation.supporting_checks} />
        <CheckList title="Failed checks" checks={recommendation.failed_checks} />
        <CheckList title="Unavailable evidence" checks={recommendation.unavailable_checks} />
      </div>
    </details>
  );
}
