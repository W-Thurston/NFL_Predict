def elo_win_probability(rating_teamA, rating_teamB):
    '''
        
    Notes:
        - Using 480 as the divisor as opposed to 400 because a study found it more accurately applied winnings & losings 
            - source: wikipedia Elo_rating_system (suggested modification)
    
    '''
    ## Probability of of winning of rating_teamA
    P_A = 1.0 / (1.0 + 10**((rating_teamB-rating_teamA)/480))

    ## Probability of of winning of rating_teamB
    P_B = 1.0 / (1.0 + 10**((rating_teamA-rating_teamB)/480))
    
    return(P_A, P_B)

def update_elo(winning_team_elo, losing_team_elo, win_or_tie=0, K=20 ):
    winners_chances, losers_chances = elo_win_probability(winning_team_elo, losing_team_elo)
    
    ## Values should either be 1 (Win), 0.5 (Tie), 0 (Loss)
    if win_or_tie == 1:
        new_winner_elo = winning_team_elo + K * (1 - winners_chances)
        new_loser_elo  = losing_team_elo  + K * (0 - losers_chances )
    elif win_or_tie == 0.5:
        new_winner_elo = winning_team_elo + K * (0.5 - winners_chances)
        new_loser_elo  = losing_team_elo  + K * (0.5 - losers_chances )
    else:
        print("!!! Something doesn't quite seem right when updating elo rankings !!!")
    
    return(new_winner_elo, new_loser_elo)