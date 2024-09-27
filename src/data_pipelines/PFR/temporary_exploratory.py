import pandas as pd
import numpy as np
from itertools import combinations, product

h2h_df.columns = ['SIM_NUM', 'NFL_TEAM', 'OPPONENT', 'H2H_SUM', 'H2H_TOTAL', 'H2H_PCT']
sim_team_regular_season.columns = ['SIM_NUM', 'WEEK_NUM', 'NFL_TEAM', 'OPPONENT', 'OUTCOME',
       'ACTUAL_OR_PROJECTED', 'CONFERENCE', 'DIVISION', 'CONFERENCE_opp',
       'DIVISION_opp', 'DIV_GAMES', 'DIV_WIN', 'DIV_LOSS', 'CONF_GAMES',
       'CONF_WIN', 'CONF_LOSS']


## Divisional Tiebreakers
def check_divisional_tiebreaker(sim_num, list_of_h2h, debug=False):
    
    length_of_h2h_list = len(list_of_h2h)
    
    if length_of_h2h_list == 1:
        ## Do nothing
        return

    ## Tier 1 tiebreaker is head-to-head (best won-lost-tied percentage in games between the clubs)
    if length_of_h2h_list == 2:
        ## The only acceptable values are 0, .5, 1; If "[]" then the teams did not meet
        h2h_win_pct = h2h_df.loc[(h2h_df['SIM_NUM']==sim_num)&
                                 (h2h_df['NFL_TEAM']==list_of_h2h[0])&
                                 (h2h_df['OPPONENT']==list_of_h2h[1]),'H2H_PCT'].values

        ## If the teams have no seen each other before and therefore returns an empty list
        if h2h_win_pct.size == 0:
            h2h_win_pct = -1
            if debug:
                print(f"> {list_of_h2h[0]} did not play against {list_of_h2h[1]} in Sim #{sim_num}")
        else:
            h2h_win_pct = h2h_win_pct[0]
        
        ## If either of the 'h2h_win_pct' below are hit, the Tier 1 tiebreaker is hit and therefore no others need to be checked
        ## If team 1 won 100% of the games played head-to-head against team 2: team 2 is moved down a rank
        if h2h_win_pct == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM']==list_of_h2h[1]),"DIV_RANK"] += 1
            return

        ## If team 1 won 0% of the games played head-to-head against team 2: team 1 is moved down a rank
        elif h2h_win_pct == 0:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM']==list_of_h2h[0]),"DIV_RANK"] += 1
            return

    elif length_of_h2h_list >= 3:
        
        dict_of_records = {}
        for team in list_of_h2h:
            dict_of_records[team] = 0

        team_combinations = list(combinations(list_of_h2h,2))
            
        for h2h in team_combinations:
            ## The only acceptable values are 0, .5, 1; If "[]" then the teams did not meet
            h2h_win_pct = h2h_df.loc[(h2h_df['SIM_NUM']==sim_num)&
                                     (h2h_df['NFL_TEAM']==h2h[0])&
                                     (h2h_df['OPPONENT']==h2h[1]),'H2H_PCT'].values

            ## If the teams have no seen each other before and therefore returns an empty list
            if h2h_win_pct.size == 0:
                h2h_win_pct = -1
                if debug:
                    print(f"> {h2h[0]} did not play against {h2h[1]} in Sim #{sim_num}")

            ## If either of the 'h2h_win_pct' below are hit, the Tier 1 tiebreaker is hit and therefore no others need to be checked
            ## If team 1 won 100% of the games played head-to-head against team 2: team 2 is moved down a rank
            if h2h_win_pct == 1:
                dict_of_records[h2h[0]] += 2
                dict_of_records[h2h[1]] -= 2

            ## If team 1 won 0% of the games played head-to-head against team 2: team 1 is moved down a rank
            elif h2h_win_pct == 0:
                dict_of_records[h2h[1]] += 2
                dict_of_records[h2h[0]] -= 2
            else:
                dict_of_records[h2h[1]] += 1
                dict_of_records[h2h[0]] += 1

        ## Find the team with the lowest number of wins
        lowest_record = min(dict_of_records.values())
        tiebreakered_teams = [key for key in dict_of_records if dict_of_records[key] == lowest_record]

        ## only make this calculation once & reuse value
        length_of_tiebreaker_teams = len(tiebreakered_teams)

        ## If only part of the group was eliminated; else all teams are still tied
        if length_of_tiebreaker_teams != length_of_h2h_list:

            ## If total teams tied was 3
            if length_of_h2h_list == 3:

                ## If only one team was eliminated, then move them to the bottom of the ranks
                if length_of_tiebreaker_teams == 1:
                    sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                                (sim_team_regular_season['NFL_TEAM']==tiebreakered_teams[0]),"DIV_RANK"] += 2
                    
                ## If more than one team was knocked out here, increase their rank (since tiebreaker was hit for atleast 1 team (above)), then re-loop over the data
                else:
                    sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                                (sim_team_regular_season['NFL_TEAM'].isin(tiebreakered_teams)),"DIV_RANK"] += 1

                    check_divisional_tiebreaker(sim_num, tiebreakered_teams, debug)

            ## If total teams tied was 4
            elif length_of_h2h_list == 4:

                ## If only one team was eliminated, then move them to the bottom of the ranks, and continue with further tiebreaker tiers
                if length_of_tiebreaker_teams == 1:
                    sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                                (sim_team_regular_season['NFL_TEAM']==tiebreakered_teams[0]),"DIV_RANK"] += 3
                
                elif length_of_tiebreaker_teams == 2:
                    sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                                (sim_team_regular_season['NFL_TEAM']==tiebreakered_teams[0]),"DIV_RANK"] += 2
                    check_divisional_tiebreaker(sim_num, tiebreakered_teams, debug)
                
                ## If more than one team was knocked out here, then re-loop over the data
                else:
                    check_divisional_tiebreaker(sim_num, tiebreakered_teams, debug)
                    
            for team in tiebreakered_teams:
                list_of_h2h.remove(team)
                length_of_h2h_list = len(list_of_h2h)

            ## If only one team remains, break
            if length_of_h2h_list == 1:
                return
            
            ## If two clubs remain tied after a third club or other clubs are eliminated during any step, the tiebreaker reverts to step 1 of the two-club format
            if length_of_h2h_list == 2:
                check_divisional_tiebreaker(sim_num, list_of_h2h, debug)
                return
            
    ## Pulling out team data to reduce the number of dataframe slices
    if length_of_h2h_list == 2:
        team_1 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[0]),:].squeeze()
        team_2 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[1]),:].squeeze()
        teams = [team_1, team_2]

    elif length_of_h2h_list == 3:
        team_1 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[0]),:].squeeze()
        team_2 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[1]),:].squeeze()
        team_3 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[2]),:].squeeze()
        teams = [team_1, team_2, team_3]
        
    elif length_of_h2h_list == 4:
        team_1 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[0]),:].squeeze()
        team_2 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[1]),:].squeeze()
        team_3 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[2]),:].squeeze()
        team_4 = sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                             (sim_team_regular_season['NFL_TEAM']==list_of_h2h[3]),:].squeeze()
        teams = [team_1, team_2, team_3, team_4]
     
    ## Tier 2 tiebreaker is best won-lost-tied percentage in games played within the division
    if length_of_h2h_list == 2:
        if team_1.DIV_PCT > team_2.DIV_PCT:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM']==team_2.NFL_TEAM),"DIV_RANK"] += 1
            return
        
        elif team_1.DIV_PCT < team_2.DIV_PCT:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM']==team_1.NFL_TEAM),"DIV_RANK"] += 1
            return

    elif length_of_h2h_list >= 3:
        team_div_pct = sorted([(x.NFL_TEAM, x.DIV_PCT) for x in teams],key= lambda x: x[1])
        lowest_div_pct = team_div_pct[0][1]
        teams_eliminated = [x[0] for x in team_div_pct if x[1] == team_div_pct[0][1]]
        length_teams_eliminated = len(teams_eliminated)

        if length_of_h2h_list == 3:
            if length_teams_eliminated == 1:
                sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                            (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2
                
                ## Pull out the non-eliminated teams
                remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

                check_divisional_tiebreaker(sim_num, remaining_teams, debug)
                return

            elif length_teams_eliminated == 2:
                sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                            (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
                    
                check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
                return

        elif length_of_h2h_list == 4:
            if length_teams_eliminated == 1:
                sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                            (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 3
                
            elif length_teams_eliminated == 2:
                sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                            (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
                
                remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

                check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
                check_divisional_tiebreaker(sim_num, remaining_teams, debug)
                return

            elif length_teams_eliminated == 3:
                sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                            (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
                remaining_teams = [x for x in list_of_h2h if x not in teams_eliminated]
                list_of_h2h = remaining_teams
                length_of_h2h_list = len(list_of_h2h)

    ## Tier 3 tiebreaker is best won-lost-tied percentage in common games             
    common_games = [x.OPPONENTS for x in teams]
    common_games = list(set(common_games[0]).intersection(*common_games))
    common_games = list(product(list_of_h2h,common_games))

    t3_tiebreaker_dict = {}
    for game in common_games:

        ## If team is not in the dict, add a new record and assign (sum of wins, total games) else add (sum of wins, total games) to existing totals
        if game[0] not in t3_tiebreaker_dict:
            t3_tiebreaker_dict[game[0]] = [h2h_df.loc[(h2h_df['SIM_NUM']==sim_num)&
                                                        (h2h_df['NFL_TEAM']==game[0])&
                                                        (h2h_df['OPPONENT']==game[1]),'H2H_SUM'].values[0],
                                            h2h_df.loc[(h2h_df['SIM_NUM']==sim_num)&
                                                        (h2h_df['NFL_TEAM']==game[0])&
                                                        (h2h_df['OPPONENT']==game[1]),'H2H_TOTAL'].values[0]]
        else:
            t3_tiebreaker_dict[game[0]][0] += h2h_df.loc[(h2h_df['SIM_NUM']==sim_num)&
                                                        (h2h_df['NFL_TEAM']==game[0])&
                                                        (h2h_df['OPPONENT']==game[1]),'H2H_SUM'].values[0]
            t3_tiebreaker_dict[game[0]][1] += h2h_df.loc[(h2h_df['SIM_NUM']==sim_num)&
                                                        (h2h_df['NFL_TEAM']==game[0])&
                                                        (h2h_df['OPPONENT']==game[1]),'H2H_TOTAL'].values[0]
    if length_of_h2h_list == 2:     
        team_1_common_games_pct = t3_tiebreaker_dict[team_1.NFL_TEAM][0]/t3_tiebreaker_dict[team_1.NFL_TEAM][1]
        team_2_common_games_pct = t3_tiebreaker_dict[team_2.NFL_TEAM][0]/t3_tiebreaker_dict[team_2.NFL_TEAM][1]  

        common_games_pct = [(team_1.NFL_TEAM, team_1_common_games_pct),
                            (team_2.NFL_TEAM, team_2_common_games_pct)] 
    
    elif length_of_h2h_list == 3:
        team_1_common_games_pct = t3_tiebreaker_dict[team_1.NFL_TEAM][0]/t3_tiebreaker_dict[team_1.NFL_TEAM][1]
        team_2_common_games_pct = t3_tiebreaker_dict[team_2.NFL_TEAM][0]/t3_tiebreaker_dict[team_2.NFL_TEAM][1]
        team_3_common_games_pct = t3_tiebreaker_dict[team_3.NFL_TEAM][0]/t3_tiebreaker_dict[team_3.NFL_TEAM][1]

        common_games_pct = [(team_1.NFL_TEAM, team_1_common_games_pct),
                            (team_2.NFL_TEAM, team_2_common_games_pct),
                            (team_3.NFL_TEAM, team_3_common_games_pct)]

    elif length_of_h2h_list == 4:
        team_1_common_games_pct = t3_tiebreaker_dict[team_1.NFL_TEAM][0]/t3_tiebreaker_dict[team_1.NFL_TEAM][1]
        team_2_common_games_pct = t3_tiebreaker_dict[team_2.NFL_TEAM][0]/t3_tiebreaker_dict[team_2.NFL_TEAM][1]
        team_3_common_games_pct = t3_tiebreaker_dict[team_3.NFL_TEAM][0]/t3_tiebreaker_dict[team_3.NFL_TEAM][1]
        team_4_common_games_pct = t3_tiebreaker_dict[team_4.NFL_TEAM][0]/t3_tiebreaker_dict[team_4.NFL_TEAM][1]

        common_games_pct = [(team_1.NFL_TEAM, team_1_common_games_pct),
                            (team_2.NFL_TEAM, team_2_common_games_pct),
                            (team_3.NFL_TEAM, team_3_common_games_pct),
                            (team_4.NFL_TEAM, team_4_common_games_pct)]
        
    common_games_pct = sorted(common_games_pct, key= lambda x: x[1])
    lowest_common_games_pct = common_games_pct[0][1]
    teams_eliminated = [x[0] for x in common_games_pct if x[1] == lowest_common_games_pct]
    length_teams_eliminated = len(teams_eliminated)

    if length_of_h2h_list == 2:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
            return
                        
    elif length_of_h2h_list == 3:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return
            
        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            return

    elif length_of_h2h_list == 4:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 3

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

            check_divisional_tiebreaker(sim_num, remaining_teams, debug)

        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return

        elif length_teams_eliminated == 3:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))

            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)

    ## Tier 4 tiebreaker is best won-lost-tied percentage in games played within the conference               
    team_conf_pct = sorted([(x.NFL_TEAM, x.CONF_PCT) for x in teams],key= lambda x: x[1])
    lowest_conf_pct = team_conf_pct[0][1]
    teams_eliminated = [x[0] for x in team_conf_pct if x[1] == lowest_conf_pct]
    length_teams_eliminated = len(teams_eliminated)

    if length_of_h2h_list == 2:
        if len(teams_eliminated) == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
            return

    elif length_of_h2h_list == 3:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return
            
        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            return

    elif length_of_h2h_list == 4:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 3

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)

        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return

        elif length_teams_eliminated == 3:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)

    ## Tier 5 tiebreaker is Strength of victory (the combined won-lost-tied percentage of all the teams that a club has defeated)            
    team_SoV = sorted([(x.NFL_TEAM, x.SOV) for x in teams],key= lambda x: x[1])
    lowest_SoV = team_SoV[0][1]
    teams_eliminated = [x[0] for x in team_SoV if x[1] == lowest_SoV]
    length_teams_eliminated = len(teams_eliminated)
    
    if length_of_h2h_list == 2:
        if len(teams_eliminated) == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
            return

    elif length_of_h2h_list == 3:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return
            
        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            return

    elif length_of_h2h_list == 4:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 3

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)

        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return

        elif length_teams_eliminated == 3:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)

    ## Tier 6 tiebreaker is Strength of schedule (the combined won-lost-tied percentage of all the teams that a club has played against)
    team_SoS = sorted([(x.NFL_TEAM, x.SOS) for x in teams],key= lambda x: x[1])
    lowest_SoS = team_SoS[0][1]
    teams_eliminated = [x[0] for x in team_SoS if x[1] == lowest_SoS]
    length_teams_eliminated = len(teams_eliminated)

    if length_of_h2h_list == 2:
        if len(teams_eliminated) == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1
            return

    elif length_of_h2h_list == 3:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return
            
        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            return
        
    elif length_of_h2h_list == 4:
        if length_teams_eliminated == 1:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 3

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)

        elif length_teams_eliminated == 2:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 2

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)
            check_divisional_tiebreaker(sim_num, remaining_teams, debug)
            return

        elif length_teams_eliminated == 3:
            sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                        (sim_team_regular_season['NFL_TEAM'].isin(teams_eliminated)),"DIV_RANK"] += 1

            ## Pull out the non-eliminated teams
            remaining_teams = list(set(list_of_h2h)-set(teams_eliminated))
            check_divisional_tiebreaker(sim_num, teams_eliminated, debug)

    ## Tier 7 tiebreaker is where "points scored" and "points allowed" comes into play. If we reach this point, for now, I will just 'flip a coin'

    random_number_list = [np.random.random() for x in range(length_of_h2h_list)]
    if length_of_h2h_list == 2:
        ## Change the 'loser'
        sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                    (sim_team_regular_season['NFL_TEAM']==list_of_h2h[np.argmin(random_number_list)]),"DIV_RANK"] += 1
        return
        
    elif length_of_h2h_list == 3:
        ## Change the "winner"
        sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                    (sim_team_regular_season['NFL_TEAM']==list_of_h2h[np.argmax(random_number_list)]),"DIV_RANK"] += 1
        ## Change the 'loser'
        sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                    (sim_team_regular_season['NFL_TEAM']==list_of_h2h[np.argmin(random_number_list)]),"DIV_RANK"] += 2
        return
        
    elif length_of_h2h_list == 4:
        ## Change the "winner"
        sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                    (sim_team_regular_season['NFL_TEAM']==list_of_h2h[np.argmax(random_number_list)]),"DIV_RANK"] += 1
        
        ## Remove the max value. This way there is a 'new' max value 
        del random_number_list[np.argmax(random_number_list)]

        ## Change the "second winner"
        sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                    (sim_team_regular_season['NFL_TEAM']==list_of_h2h[np.argmax(random_number_list)]),"DIV_RANK"] += 2
        ## Change the 'loser'
        sim_team_regular_season.loc[(sim_team_regular_season['SIM_NUM']==sim_num)&
                                    (sim_team_regular_season['NFL_TEAM']==list_of_h2h[np.argmin(random_number_list)]),"DIV_RANK"] += 3
        return

## Divisional Rank
sim_team_regular_season['DIV_RANK'] = sim_team_regular_season.loc[:,['SIM_NUM','DIVISION','NFL_TEAM','WIN_PCT']].groupby(['SIM_NUM','DIVISION'])['WIN_PCT'].rank(method='min', ascending=False)
print("> Finished calculating division rankings")

checking_for_ties = sim_team_regular_season.loc[:,['SIM_NUM','DIVISION','NFL_TEAM','DIV_RANK']].groupby(['SIM_NUM','DIVISION','DIV_RANK']).agg({'NFL_TEAM':lambda x: list(x)})
checking_for_ties['TEAM_COMBOS'] = checking_for_ties.apply(lambda x: list(x.NFL_TEAM),axis=1)
checking_for_ties = checking_for_ties.reset_index()
print("> Finished calculating Ties")

tqdm.pandas(desc=f"{'Working through divisional tiebreakers: '}")
_ = checking_for_ties.progress_apply(lambda x: check_divisional_tiebreaker(x.SIM_NUM, x.TEAM_COMBOS, debug=False), axis=1).values
_ = None
del _