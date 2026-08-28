import { useId } from "react";
import type { BetLeg } from "../../context/BetSlipContext";
import {
  analyzeBetLeg,
  type BetLegAnalysis,
} from "../../utils/betLegs";
import {
  formatOdds,
} from "../../utils/odds";
import {
  formatStatType,
} from "../../utils/props";
import { PendingChip } from "../field-status/PendingChip";
import { TeamMark } from "../primitives/TeamMark";
import {
  formatAllocationReason,
  formatSuggestedStake,
  isRecommendationEligible,
  recommendationPresentation,
  recommendationToneColor,
  assertNever,
} from "../recommendations/recommendationPresentation";
import type { RecommendationTone } from "../recommendations/recommendationPresentation";

type OddsFormat =
  | "american"
  | "decimal";

type BetLegCardProps = {
  leg: BetLeg;
  oddsFormat: OddsFormat;
  bankroll: number | null;
  kellyMultiplier: number;
  onUpdateCurrentOdds: (
    value: number | null,
  ) => void;
  onUpdateProposedStake: (
    value: number | null,
  ) => void;
  onUpdateSportsbook: (
    value: string | null,
  ) => void;
  onUpdateNote: (
    value: string | null,
  ) => void;
  onRemove: () => void;
  onRecordWager?: () => void;
  isRecording?: boolean;
  recordingDisabled?: boolean;
};

export function BetLegCard({
  leg,
  oddsFormat,
  bankroll,
  kellyMultiplier,
  onUpdateCurrentOdds,
  onUpdateProposedStake,
  onUpdateSportsbook,
  onUpdateNote,
  onRemove,
  onRecordWager,
  isRecording = false,
  recordingDisabled = false,
}: BetLegCardProps) {
  const fieldIdPrefix = useId();
  const accessibleLegLabel =
    legLabel(leg);

  const analysis = analyzeBetLeg({
    leg,
    bankroll,
    kellyMultiplier,
  });

  return (
    <article
      aria-label={accessibleLegLabel}
      style={{
        padding: 14,
        backgroundColor: "var(--bg-1)",
        border:
          "1px solid var(--line-soft)",
        borderRadius: 6,
      }}
    >
      <CardHeader
        leg={leg}
        onRemove={onRemove}
      />

      <WagerDescription leg={leg} />

      <PersistedPolicyResultSection
        leg={leg}
        accessibleLegLabel={accessibleLegLabel}
      />

      <PriceSection
        leg={leg}
        analysis={analysis}
        oddsFormat={oddsFormat}
        fieldId={`${fieldIdPrefix}-current-odds`}
        accessibleLegLabel={
          accessibleLegLabel
        }
        onUpdateCurrentOdds={
          onUpdateCurrentOdds
        }
      />

      <ModelSection
        leg={leg}
        analysis={analysis}
        oddsFormat={oddsFormat}
        kellyMultiplier={
          kellyMultiplier
        }
      />

      <StakeSection
        leg={leg}
        analysis={analysis}
        fieldId={`${fieldIdPrefix}-proposed-stake`}
        accessibleLegLabel={
          accessibleLegLabel
        }
        onUpdateProposedStake={
          onUpdateProposedStake
        }
      />

      <DraftDetails
        leg={leg}
        sportsbookId={`${fieldIdPrefix}-sportsbook`}
        noteId={`${fieldIdPrefix}-note`}
        accessibleLegLabel={
          accessibleLegLabel
        }
        onUpdateSportsbook={
          onUpdateSportsbook
        }
        onUpdateNote={onUpdateNote}
      />

      {leg.kind === "game" ? (
        onRecordWager && (
          <button
            type="button"
            onClick={onRecordWager}
            disabled={recordingDisabled || isRecording}
            aria-label={`Record ${accessibleLegLabel} in Gridiron Edge`}
            style={{
              width: "100%",
              marginTop: 12,
              padding: "8px 12px",
              border: "none",
              borderRadius: 4,
              background: recordingDisabled || isRecording
                ? "var(--bg-3)"
                : "var(--pos)",
              color: recordingDisabled || isRecording
                ? "var(--ink-4)"
                : "var(--bg)",
              cursor: recordingDisabled || isRecording
                ? "not-allowed"
                : "pointer",
              fontFamily: "var(--f-sans)",
              fontWeight: 600,
            }}
          >
            {isRecording ? "Recording..." : "Record wager"}
          </button>
        )
      ) : (
        <div className="mono dim2" style={{ marginTop: 10, fontSize: 9 }}>
          Recording is currently available for Moneyline, Spread, and Total
          game wagers.
        </div>
      )}
    </article>
  );
}

function CardHeader({
  leg,
  onRemove,
}: {
  leg: BetLeg;
  onRemove: () => void;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        gap: 12,
        marginBottom: 8,
      }}
    >
      <div
        style={{
          minWidth: 0,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        {leg.kind === "game" ? (
          <>
            <TeamMark
              abbr={leg.awayTeam}
              size={20}
            />

            <span className="dim">@</span>

            <TeamMark
              abbr={leg.homeTeam}
              size={20}
            />
          </>
        ) : (
          <>
            <TeamMark
              abbr={leg.team}
              size={20}
            />

            <div>
              <div
                style={{
                  fontSize: 12,
                  color: "var(--ink)",
                }}
              >
                {leg.playerName}
              </div>

              <div
                className="mono dim2"
                style={{ fontSize: 9 }}
              >
                {leg.position} ·{" "}
                {leg.team}
              </div>
            </div>
          </>
        )}
      </div>

      <button
        type="button"
        onClick={onRemove}
        aria-label={`Remove ${legLabel(
          leg,
        )}`}
        title="Remove leg"
        style={{
          backgroundColor:
            "transparent",
          border: "none",
          padding: "1px 4px",
          fontSize: 16,
          lineHeight: 1,
          color: "var(--ink-3)",
          cursor: "pointer",
          flexShrink: 0,
        }}
      >
        ×
      </button>
    </div>
  );
}

function PersistedPolicyResultSection({
  leg,
  accessibleLegLabel,
}: {
  leg: BetLeg;
  accessibleLegLabel: string;
}) {
  const persisted = leg.persistedRecommendation;
  const presentation = recommendationPresentation(persisted);
  const suggestedStake = formatSuggestedStake(persisted);
  const allocationReason = formatAllocationReason(persisted);

  return (
    <section
      aria-label={`Persisted policy result for ${accessibleLegLabel}`}
      style={{ marginBottom: 12, display: "grid", gap: 4 }}
    >
      <span
        className="mono"
        style={{ fontSize: 11, fontWeight: 600, color: recommendationToneColor(presentation.tone) }}
      >
        {presentation.label}
      </span>
      <span className="mono dim2" style={{ fontSize: 9.5 }}>
        {presentation.description}
      </span>
      {suggestedStake && (
        <span className="mono dim2" style={{ fontSize: 9.5 }}>
          Governed suggested stake: {suggestedStake}
        </span>
      )}
      {allocationReason && (
        <span className="mono dim2" style={{ fontSize: 9.5 }}>
          {allocationReason}
        </span>
      )}
    </section>
  );
}

// BetLegCard.tsx
function toMetricTone(
  tone: RecommendationTone,
): "default" | "positive" | "warning" | "negative" {
  switch (tone) {
    case "positive":
      return "positive";
    case "warning":
    case "unavailable":
      return "warning";
    case "negative":
    case "conflicting":
      return "negative";
    case "candidate":
      return "default";
    default:
      return assertNever(tone);
  }
}

function WagerDescription({
  leg,
}: {
  leg: BetLeg;
}) {
  return (
    <div
      style={{
        marginBottom: 12,
        paddingBottom: 10,
        borderBottom:
          "1px solid var(--line-soft)",
      }}
    >
      <div
        className="mono"
        style={{
          fontSize: 11,
          color: "var(--ink-2)",
          textTransform: "capitalize",
        }}
      >
        {leg.kind === "game"
          ? gameDescription(leg)
          : propDescription(leg)}
      </div>

      <div
        className="mono dim2"
        style={{
          marginTop: 3,
          fontSize: 9,
        }}
      >
        Evidence:{" "}
        {leg.edgeAnalytics?.modelKey ?? "Unavailable"}
      </div>
    </div>
  );
}

function PriceSection({
  leg,
  analysis,
  oddsFormat,
  fieldId,
  accessibleLegLabel,
  onUpdateCurrentOdds,
}: {
  leg: BetLeg;
  analysis: BetLegAnalysis;
  oddsFormat: OddsFormat;
  fieldId: string;
  accessibleLegLabel: string;
  onUpdateCurrentOdds: (
    value: number | null,
  ) => void;
}) {
  const referenceOdds =
    leg.edgeAnalytics
      ?.referenceAmericanOdds ?? null;

  const currentOdds =
    leg.draft.currentAmericanOdds;

  return (
    <section
      aria-label={`Price comparison for ${accessibleLegLabel}`}
      className="betslip-card-grid"
      style={{
        marginBottom: 12,
      }}
    >
      <FieldBlock label="Reference price">
        {referenceOdds == null ? (
          <PendingChip>
            Reference price unavailable
          </PendingChip>
        ) : (
          <span className="mono tnum">
            {formatOdds(
              referenceOdds,
              oddsFormat,
            )}
          </span>
        )}
      </FieldBlock>

      <FieldBlock label="Current price">
        <AmericanOddsInput
          id={fieldId}
          label={`Current American odds for ${accessibleLegLabel}`}
          value={currentOdds}
          onChange={
            onUpdateCurrentOdds
          }
        />
      </FieldBlock>

      <FieldBlock label="Model break-even">
        {analysis
          .breakEvenAmericanOdds ==
        null ? (
          <span className="mono dim2">
            Unavailable
          </span>
        ) : (
          <span className="mono tnum">
            {formatOdds(
              analysis
                .breakEvenAmericanOdds,
              oddsFormat,
            )}
          </span>
        )}
      </FieldBlock>

      <FieldBlock label="Price status">
        <PriceStatus
          analysis={analysis}
        />
      </FieldBlock>
    </section>
  );
}

function ModelSection({
  leg,
  analysis,
  oddsFormat,
  kellyMultiplier,
}: {
  leg: BetLeg;
  analysis: BetLegAnalysis;
  oddsFormat: OddsFormat;
  kellyMultiplier: number;
}) {
  const probability = leg.edgeAnalytics?.referenceModelProbability ?? null;

  const persisted = leg.persistedRecommendation;
  const governedPresentation = persisted == null ? null : recommendationPresentation(persisted);
  const governedStake = formatSuggestedStake(persisted);
  const allocationReason = formatAllocationReason(persisted);

  return (
    <section
      aria-label={`Model analysis for ${legLabel(leg)}`}
      className="betslip-card-grid"
      style={{
        marginBottom: 12,
        padding: 10,
        backgroundColor: "var(--bg-2)",
        borderRadius: 5,
      }}
    >
      <Metric
        label="Model probability"
        value={probability == null ? null : `${(probability * 100).toFixed(1)}%`}
      />

      <Metric
        label="Edge strength"
        value={leg.edgeAnalytics?.referenceEdgeStrength ?? null}
        capitalize
      />

      <Metric
        label="Reference EV"
        value={formatPercent(leg.edgeAnalytics?.referenceExpectedValue ?? null)}
        tone={evTone(leg.edgeAnalytics?.referenceExpectedValue ?? null)}
      />

      <Metric
        label="Current EV"
        value={formatPercent(analysis.current?.expectedValue ?? null)}
        tone={evTone(analysis.current?.expectedValue ?? null)}
      />

      <Metric
        label="Full Kelly"
        value={formatPercent(analysis.current?.fullKellyFraction ?? null)}
      />

      <Metric
        label={`${kellyMultiplier.toFixed(2)}× suggested stake`}
        value={formatMoney(analysis.suggestedStake)}
      />

      {leg.kind === "prop" && (
        <Metric
          label="Model projection"
          value={leg.predictedMean == null ? null : leg.predictedMean.toFixed(1)}
        />
      )}

      <Metric
        label="Current implied"
        value={formatPercent(analysis.current?.impliedProbability ?? null)}
      />

      {analysis.current && leg.draft.currentAmericanOdds != null && (
        <div className="mono dim2" style={{ gridColumn: "1 / -1", fontSize: 9 }}>
          Current price {formatOdds(leg.draft.currentAmericanOdds, oddsFormat)} is used for
          current EV, Kelly, payout, and profit.
        </div>
      )}

      {governedPresentation && (
        <div
          style={{
            gridColumn: "1 / -1",
            marginTop: 8,
            paddingTop: 8,
            borderTop: "1px solid var(--line-soft)",
          }}
        >
          <div
            className="upper dim2"
            style={{ fontSize: 9, marginBottom: 6, letterSpacing: "0.06em" }}
          >
            {isRecommendationEligible(persisted) ? "Governed Recommendation" : "Persisted Policy Result"}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            <Metric
              label="Policy state"
              value={governedPresentation.label}
              tone={toMetricTone(governedPresentation.tone)}
            />
            <Metric label="Governed suggested stake" value={governedStake} />
          </div>
          {allocationReason && (
            <div className="mono dim2" style={{ fontSize: 9, marginTop: 6 }}>
              {allocationReason}
            </div>
          )}
        </div>
      )}
    </section>
  );
}

function StakeSection({
  leg,
  analysis,
  fieldId,
  accessibleLegLabel,
  onUpdateProposedStake,
}: {
  leg: BetLeg;
  analysis: BetLegAnalysis;
  fieldId: string;
  accessibleLegLabel: string;
  onUpdateProposedStake: (
    value: number | null,
  ) => void;
}) {
  return (
    <section
      aria-label={`Stake and payout for ${accessibleLegLabel}`}
      className="betslip-card-grid betslip-card-grid--stake"
      style={{
        marginBottom: 12,
      }}
    >
      <FieldBlock label="Your proposed stake">
        <MoneyInput
          id={fieldId}
          label={`Proposed stake for ${accessibleLegLabel}`}
          value={
            leg.draft.proposedStake
          }
          onChange={
            onUpdateProposedStake
          }
        />
      </FieldBlock>

      <Metric
        label="Potential payout"
        value={formatMoney(
          analysis.payout,
        )}
      />

      <Metric
        label="Potential profit"
        value={formatMoney(
          analysis.profit,
          true,
        )}
        tone={
          analysis.profit == null
            ? "default"
            : analysis.profit >= 0
              ? "positive"
              : "negative"
        }
      />
    </section>
  );
}

function DraftDetails({
  leg,
  sportsbookId,
  noteId,
  accessibleLegLabel,
  onUpdateSportsbook,
  onUpdateNote,
}: {
  leg: BetLeg;
  sportsbookId: string;
  noteId: string;
  accessibleLegLabel: string;
  onUpdateSportsbook: (
    value: string | null,
  ) => void;
  onUpdateNote: (
    value: string | null,
  ) => void;
}) {
  return (
    <details>
      <summary
        className="mono dim"
        style={{
          fontSize: 10,
          cursor: "pointer",
        }}
      >
        Draft details
      </summary>

      <div
        style={{
          display: "grid",
          gap: 8,
          marginTop: 10,
        }}
      >
        <label
          htmlFor={sportsbookId}
          className="upper dim2"
          style={{ fontSize: 9 }}
        >
          Sportsbook for{" "}
          {accessibleLegLabel}
          <input
            id={sportsbookId}
            type="text"
            value={
              leg.draft.sportsbook ??
              ""
            }
            placeholder="Optional manual entry"
            onChange={(event) =>
              onUpdateSportsbook(
                nullableText(
                  event.target.value,
                ),
              )
            }
            style={textInputStyle}
          />
        </label>

        <label
          htmlFor={noteId}
          className="upper dim2"
          style={{ fontSize: 9 }}
        >
          Note for {accessibleLegLabel}
          <textarea
            id={noteId}
            value={leg.draft.note ?? ""}
            placeholder="Optional draft note"
            rows={2}
            onChange={(event) =>
              onUpdateNote(
                nullableText(
                  event.target.value,
                ),
              )
            }
            style={{
              ...textInputStyle,
              resize: "vertical",
            }}
          />
        </label>
      </div>
    </details>
  );
}

function AmericanOddsInput({
  id,
  label,
  value,
  onChange,
}: {
  id: string;
  label: string;
  value: number | null;
  onChange: (
    value: number | null,
  ) => void;
}) {
  return (
    <input
      id={id}
      aria-label={label}
      type="number"
      step={1}
      value={value ?? ""}
      placeholder="Enter odds"
      onChange={(event) =>
        onChange(
          numberOrNull(
            event.target.value,
          ),
        )
      }
      style={numberInputStyle}
    />
  );
}

function MoneyInput({
  id,
  label,
  value,
  onChange,
}: {
  id: string;
  label: string;
  value: number | null;
  onChange: (
    value: number | null,
  ) => void;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 4,
      }}
    >
      <span className="mono dim">
        $
      </span>

      <input
        id={id}
        aria-label={label}
        type="number"
        min={0}
        step={5}
        value={value ?? ""}
        placeholder="0.00"
        onChange={(event) =>
          onChange(
            numberOrNull(
              event.target.value,
            ),
          )
        }
        style={numberInputStyle}
      />
    </div>
  );
}

function PriceStatus({
  analysis,
}: {
  analysis: BetLegAnalysis;
}) {
  if (
    analysis.currentPriceIsAcceptable ==
    null
  ) {
    return (
      <span className="mono dim2">
        Threshold unavailable
      </span>
    );
  }

  if (
    analysis.currentPriceIsAcceptable
  ) {
    return (
      <span
        className="mono"
        style={{
          color: "var(--pos)",
        }}
      >
        Positive modeled EV
      </span>
    );
  }

  return (
    <span
      className="mono"
      style={{
        color: "var(--warn)",
      }}
    >
      Below model threshold
    </span>
  );
}

function FieldBlock({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div
        className="upper dim2"
        style={{
          marginBottom: 4,
          fontSize: 9,
        }}
      >
        {label}
      </div>

      <div style={{ fontSize: 12 }}>
        {children}
      </div>
    </div>
  );
}

function Metric({
  label,
  value,
  tone = "default",
  capitalize = false,
}: {
  label: string;
  value: string | null;
  tone?:
    | "default"
    | "positive"
    | "warning"
    | "negative";
  capitalize?: boolean;
}) {
  const color =
    tone === "positive"
      ? "var(--pos)"
      : tone === "warning"
        ? "var(--warn)"
        : tone === "negative"
          ? "var(--neg)"
          : value == null
            ? "var(--ink-4)"
            : "var(--ink-2)";

  return (
    <div>
      <div
        className="upper dim2"
        style={{
          marginBottom: 4,
          fontSize: 9,
        }}
      >
        {label}
      </div>

      <div
        className="mono tnum"
        style={{
          fontSize: 12,
          color,
          textTransform:
            capitalize
              ? "capitalize"
              : undefined,
        }}
      >
        {value ?? "Unavailable"}
      </div>
    </div>
  );
}

function legLabel(
  leg: BetLeg,
): string {
  return leg.kind === "game"
    ? `${leg.awayTeam} at ${leg.homeTeam} ${leg.market} ${leg.side}`
    : `${leg.playerName} ${formatStatType(
        leg.statType,
      )} ${leg.side}`;
}

function gameDescription(
  leg: Extract<
    BetLeg,
    { kind: "game" }
  >,
): string {
  const line =
    leg.line == null
      ? ""
      : ` · ${formatLine(
          leg.line,
        )}`;

  return `${leg.market} · ${leg.side}${line}`;
}

function propDescription(
  leg: Extract<
    BetLeg,
    { kind: "prop" }
  >,
): string {
  const line =
    leg.line == null
      ? ""
      : ` · ${formatLine(
          leg.line,
        )}`;

  return `${formatStatType(
    leg.statType,
  )} · ${leg.side}${line}`;
}

function formatLine(
  value: number,
): string {
  return value > 0
    ? `+${value}`
    : String(value);
}

function formatPercent(
  value: number | null,
): string | null {
  if (value == null) {
    return null;
  }

  const sign = value > 0 ? "+" : "";

  return `${sign}${(
    value * 100
  ).toFixed(1)}%`;
}

function formatMoney(
  value: number | null,
  includeSign = false,
): string | null {
  if (value == null) {
    return null;
  }

  const sign =
    includeSign && value > 0
      ? "+"
      : "";

  return `${sign}$${value.toFixed(2)}`;
}

function evTone(
  value: number | null,
):
  | "default"
  | "positive"
  | "warning" {
  if (value == null) {
    return "default";
  }

  if (value > 0) {
    return "positive";
  }

  return "warning";
}

function numberOrNull(
  value: string,
): number | null {
  if (value.trim() === "") {
    return null;
  }

  const parsed = Number(value);

  return Number.isFinite(parsed)
    ? parsed
    : null;
}

function nullableText(
  value: string,
): string | null {
  const normalized = value.trim();

  return normalized === ""
    ? null
    : normalized;
}

const numberInputStyle:
  React.CSSProperties = {
    width: "100%",
    minWidth: 0,
    boxSizing: "border-box",
    padding: "6px 8px",
    backgroundColor: "var(--bg-2)",
    color: "var(--ink)",
    border:
      "1px solid var(--line-soft)",
    borderRadius: 4,
    fontFamily: "var(--f-mono)",
    fontVariantNumeric:
      "tabular-nums",
  };

const textInputStyle:
  React.CSSProperties = {
    display: "block",
    width: "100%",
    boxSizing: "border-box",
    marginTop: 4,
    padding: "7px 8px",
    backgroundColor: "var(--bg-2)",
    color: "var(--ink)",
    border:
      "1px solid var(--line-soft)",
    borderRadius: 4,
    fontFamily: "var(--f-sans)",
    fontSize: 12,
  };
