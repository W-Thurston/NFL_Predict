import type { components } from "../../api/schema";
import type { GameBetLeg } from "../../utils/betLegs";

export type RecordBetRequest = components["schemas"]["RecordBetRequest"];

function isNonemptyIdentity(value: string | null | undefined): value is string {
  return (value?.trim().length ?? 0) > 0;
}

export function buildRecordBetRequest(leg: GameBetLeg): RecordBetRequest | null {
  const odds = leg.draft.currentAmericanOdds;
  const stake = leg.draft.proposedStake;
  const book = leg.draft.sportsbook?.trim();

  if (odds == null || stake == null || stake <= 0 || !book) {
    return null;
  }

  const persisted = leg.persistedRecommendation;

  const hasCompleteRecommendation =
    persisted != null &&
    isNonemptyIdentity(persisted.result_id) &&
    isNonemptyIdentity(persisted.evaluation_id) &&
    isNonemptyIdentity(persisted.offer_provenance.candidate_reference_id) &&
    isNonemptyIdentity(persisted.policy_provenance.policy_id);

  return {
    game_id: leg.gameId,
    market_type: leg.market,
    side: leg.side,
    line: leg.line,
    odds,
    stake,
    book,
    recommended_bet_result_id: hasCompleteRecommendation ? persisted.result_id : null,
    recommendation_evaluation_id: hasCompleteRecommendation ? persisted.evaluation_id : null,
    candidate_reference_id: hasCompleteRecommendation
      ? persisted.offer_provenance.candidate_reference_id
      : null,
    recommendation_policy_id: hasCompleteRecommendation
      ? persisted.policy_provenance.policy_id
      : null,
  };
}
