import pandas as pd
import numpy as np

import re
from geopy.distance import distance
from tqdm import tqdm
tqdm.pandas()

import datetime
import timezonefinder, pytz

import utils.elo as elo
import utils.stadium_loc_dist_calc as dist_calc

class model_input_feature_builder(object):

    def __init__(self):

        ## Data Filenames
        self.pfr_cleaned_data_file = 'data/cleaned/NFL_wk_by_wk_cleaned.csv'
        self.ELO_data_file         = 'data/cleaned/NFL_Team_Elo.csv'
        self.modeling_base_file    = 'data/modeling/base_modeling_file.csv'
        self.modeling_file         = 'data/modeling/modeling_file.csv'
        self.stadium_file          = 'data/cleaned/NFL_stadium_reference.csv'

    
    def prep_data_modeling_file(self):
        print(f"> Reading in cleaned PFR data from: {self.pfr_cleaned_data_file}")
        game_data = pd.read_csv(self.pfr_cleaned_data_file)
        
        ## Create a list of sorted years, then set that as the sorting order for 'YEAR' as a categorical feature
        game_data['GAME_DATE'] = pd.to_datetime(game_data['GAME_DATE'])
        sorted_years = sorted(game_data['YEAR'].unique())
        game_data['YEAR'] = pd.Categorical(game_data['YEAR'], sorted_years)

        ## Clean up after yourself
        sorted_years = None
        del sorted_years

        ## Set the modeling target variable to '1' for winners
        game_data = game_data.loc[:,['GAME_ID','WINNER','LOSER','YEAR','WEEK_NUM']]
        game_data['RESULT'] = 1
        game_data.columns = ['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM', 'RESULT']

        ## Flip the winners and losers so that we can concat this data to the above and set modeling target to 0
        game_data_flipped = game_data.loc[:,['GAME_ID','TEAM_B','TEAM_A','YEAR','WEEK_NUM']]
        game_data_flipped['RESULT'] = 0
        game_data_flipped.columns = ['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM', 'RESULT']

        ## Concatenate the two DataFrames
        game_data = pd.concat([game_data, game_data_flipped]).sort_values(['YEAR','WEEK_NUM','TEAM_A'])

        ## Clean up after yourself
        game_data_flipped = None
        del game_data_flipped

        ## Now you have an initial DataFrame in the form [Team_A, Team_B, Year, Week_num, Result]
        #   This will be useful for appending modeling features to.  Also, mostly for testing purposes
        game_data.to_csv(self.modeling_base_file, index=False)
        print(f"> Saving modeling base file to: {self.modeling_base_file}")
        print()

    @staticmethod
    def _build_PFR_ELO_features(self, all_data):

        if all_data:
            print(f"> Reading in cleaned PFR data from: {self.pfr_cleaned_data_file}")
            df = pd.read_csv(self.pfr_cleaned_data_file)
            print(f"> Generating all ELO data")

            ## Pull out all NFL teams
            NFL_TEAMS = sorted(df['WINNER'].unique())

            ## Pull out Years and Week numbers
            NFL_YEARS = sorted(df['YEAR'].unique())
            NFL_WEEKS = sorted(df['WEEK_NUM'].unique())

            ## Game Date to datetime
            df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

            ## Create sorted year list like '91-92' to use as categorical ordering list
            sorted_years = sorted(df['YEAR'].unique())
            increase_in_number_of_weeks_in_season = sorted_years.index('2021-2022')

            dataframe_builder_list = []
            for team in NFL_TEAMS:
                for year in NFL_YEARS:
                    for week in NFL_WEEKS:
                        dataframe_builder_list.append( (team, year, week) )

            df_team_elo = pd.DataFrame.from_records(dataframe_builder_list, columns=['NFL_TEAM', 'NFL_YEAR', 'NFL_WEEK']).drop_duplicates()
            df_team_elo['ELO'] = np.nan
            df_team_elo.loc[(df_team_elo['NFL_YEAR']==sorted_years[0])&(df_team_elo['NFL_WEEK']==1),'ELO'] = 1500

            dataframe_builder_list = None

            ## Make dataframe year into categories
            df_team_elo['NFL_YEAR'] = pd.Categorical(df_team_elo['NFL_YEAR'], sorted_years)

            ## Create lists of year numbers to denote different length seasons
            seasons_with_21_weeks = sorted_years[:increase_in_number_of_weeks_in_season]
            seasons_with_21_weeks.remove('1993-1994') ## This year had an extra bye week

            ## Remove week 22 from seasons prior to '21-22'
            df_team_elo = df_team_elo.loc[~((df_team_elo['NFL_YEAR'].isin(seasons_with_21_weeks))&
                                            (df_team_elo['NFL_WEEK']==22))]
            
            ## Sort Dataframe
            df_team_elo = df_team_elo.sort_values(['NFL_YEAR','NFL_WEEK','NFL_TEAM'], ignore_index=True)

            ## Remove rows where teams didn't 'exist' yet
            #   Ex: Carolina Panthers didn't start until the 1995 season, remove all years prior

            ## Carolina Panthers
            carolina_panthers_start_year = '1995-1996'
            carolina_panthers_year_list_to_remove = df_team_elo[(df_team_elo['NFL_TEAM']=='Carolina Panthers')&
                                                                (df_team_elo['NFL_YEAR'].isin(sorted_years[:sorted_years.index(carolina_panthers_start_year)]))].index.values

            ## Jacksonville Jaguars
            jacksonville_jaguars_start_year = '1995-1996'
            jacksonville_jaguars_year_list_to_remove = df_team_elo[(df_team_elo['NFL_TEAM']=='Jacksonville Jaguars')&
                                                                (df_team_elo['NFL_YEAR'].isin(sorted_years[:sorted_years.index(jacksonville_jaguars_start_year)]))].index.values

            ## Baltimore Ravens
            baltimore_ravens_start_year = '1996-1997'
            baltimore_ravens_year_list_to_remove = df_team_elo[(df_team_elo['NFL_TEAM']=='Baltimore Ravens')&
                                                            (df_team_elo['NFL_YEAR'].isin(sorted_years[:sorted_years.index(baltimore_ravens_start_year)]))].index.values

            ## Houston Texans
            houston_texans_start_year = '2002-2003'
            houston_texans_year_list_to_remove = df_team_elo[(df_team_elo['NFL_TEAM']=='Houston Texans')&
                                                            (df_team_elo['NFL_YEAR'].isin(sorted_years[:sorted_years.index(houston_texans_start_year)]))].index.values

            idx_to_remove = np.concatenate( (carolina_panthers_year_list_to_remove,
                                            jacksonville_jaguars_year_list_to_remove,
                                            baltimore_ravens_year_list_to_remove, 
                                            houston_texans_year_list_to_remove), axis=None)

            df_team_elo.drop(idx_to_remove, inplace=True)
            
            ## Create a new column of Elo per week of a season
            print(f"> Filling out ELO column")
            k=20
            for idx, yr in enumerate(tqdm(sorted_years)):
                curr_year = yr
                if curr_year != sorted_years[-1]:
                    next_year = sorted_years[idx+1]
                else: 
                    next_year = None

                number_of_weeks_in_curr_year = len(df.loc[df['YEAR'] == curr_year, 'WEEK_NUM'].unique())

                ## pull out all team names this season, we want to create a list to check against when a new team joins the NFL
                teams_this_season = df_team_elo.loc[(df_team_elo['NFL_YEAR'] == curr_year),'NFL_TEAM'].unique()

                for wk in range(1,number_of_weeks_in_curr_year+1):
                    for i in df.loc[((df['WEEK_NUM']==wk)&(df['YEAR']==curr_year)),['WINNER','LOSER', 'WIN_OR_TIE']].iterrows():

                        winning_team_name = i[1]['WINNER']
                        losing_team_name  = i[1]['LOSER']
                        
                        ## Calculate the winner's and loser's new Elo ranking.
                        winner_elo, loser_elo = elo.update_elo( df_team_elo.loc[(df_team_elo['NFL_TEAM'] == winning_team_name )&
                                                                                (df_team_elo['NFL_YEAR'] == curr_year         )&
                                                                                (df_team_elo['NFL_WEEK'] == wk                ),'ELO'].values[0],
                                                                df_team_elo.loc[(df_team_elo['NFL_TEAM'] == losing_team_name  )&
                                                                                (df_team_elo['NFL_YEAR'] == curr_year         )&
                                                                                (df_team_elo['NFL_WEEK'] == wk                ),'ELO'].values[0],
                                                                win_or_tie= i[1]['WIN_OR_TIE'],
                                                                K=k)

                        ## Update Elo for next season's week 1
                        if (wk == number_of_weeks_in_curr_year) and (next_year is not None):
                            ## Update winning team's ELO
                            df_team_elo.loc[(df_team_elo['NFL_TEAM'] == winning_team_name)&
                                            (df_team_elo['NFL_YEAR'] == next_year        )&
                                            (df_team_elo['NFL_WEEK'] == 1               ),'ELO'] = winner_elo

                            ## Update losing team's ELO
                            df_team_elo.loc[(df_team_elo['NFL_TEAM'] == losing_team_name )&
                                            (df_team_elo['NFL_YEAR'] == next_year        )&
                                            (df_team_elo['NFL_WEEK'] == 1               ),'ELO'] = loser_elo

                        ## Update Elo for the next week in current season
                        else:

                            ## Update winning team's ELO
                            df_team_elo.loc[(df_team_elo['NFL_TEAM'] == winning_team_name)&
                                            (df_team_elo['NFL_YEAR'] == curr_year        )&
                                            (df_team_elo['NFL_WEEK'] == wk+1             ),'ELO'] = winner_elo

                            ## Update losing team's ELO
                            df_team_elo.loc[(df_team_elo['NFL_TEAM'] == losing_team_name )&
                                            (df_team_elo['NFL_YEAR'] == curr_year        )&
                                            (df_team_elo['NFL_WEEK'] == wk+1             ),'ELO'] = loser_elo

                            

                    ## Set week 1's ELO of next year equal to the ELO of the last week of the current year
                    if (wk == number_of_weeks_in_curr_year) and (next_year is not None):
                        ## Get boolean values for next year, week 1, and ELO is null
                        null_idx = df_team_elo.loc[(df_team_elo['NFL_YEAR'] == next_year)&(df_team_elo['NFL_WEEK'] == 1),'ELO'].isnull()
                        
                        ## Take list of unique team names that appear in the above null_idx list
                        teams_with_no_game_this_week = df_team_elo.loc[(df_team_elo['NFL_YEAR'] == next_year)&(df_team_elo['NFL_WEEK'] == 1),:].loc[null_idx,'NFL_TEAM'].unique()

                        ## For each team that has a null value for their ELO ranking for Next year's week 1
                        for team in teams_with_no_game_this_week:
                            
                            ## If the team has just joined the NFL, set its' ELO to 1300 (starting_value)
                            if team not in teams_this_season:
                                df_team_elo.loc[(df_team_elo['NFL_TEAM'] == team     )&
                                                (df_team_elo['NFL_YEAR'] == next_year)&
                                                (df_team_elo['NFL_WEEK'] == 1        ),'ELO'] = 1300
                            
                            else:
                                
                                ## Set the ELO value of next year's week 1 (the null value) to the value of the last week of last season (should be a float, not null)
                                df_team_elo.loc[(df_team_elo['NFL_TEAM'] == team     )&
                                                (df_team_elo['NFL_YEAR'] == next_year)&
                                                (df_team_elo['NFL_WEEK'] == 1        ),'ELO'] = df_team_elo.loc[(df_team_elo['NFL_TEAM'] == team                        )&
                                                                                                                (df_team_elo['NFL_YEAR'] == curr_year                   )&
                                                                                                                (df_team_elo['NFL_WEEK'] == number_of_weeks_in_curr_year),'ELO'].values[0]

                    ## This catches bye weeks and not making the playoffs  (so they would not have a game to iterate over in the winner/loser elo calculation) 
                    else:
                        ## Get boolean values for current year, next week (wk+1), and ELO is null
                        null_idx = df_team_elo.loc[(df_team_elo['NFL_YEAR'] == curr_year)&(df_team_elo['NFL_WEEK'] == wk+1),'ELO'].isnull()

                        ## Take list of unique team names that appear in the above null_idx list
                        teams_with_no_game_this_week = df_team_elo.loc[(df_team_elo['NFL_YEAR'] == curr_year)&(df_team_elo['NFL_WEEK'] == wk+1),:].loc[null_idx,'NFL_TEAM'].unique()
                        
                        ## For each team that has a null value for their ELO ranking for the next week
                        for team in teams_with_no_game_this_week:
                            
                            ## Set the ELO value of next week (the null value) to the value of last week (should be a float, not null)
                            df_team_elo.loc[(df_team_elo['NFL_TEAM'] == team        )&
                                            (df_team_elo['NFL_YEAR'] == curr_year   )&
                                            (df_team_elo['NFL_WEEK'].isin([wk,wk+1])),'ELO'] = df_team_elo.loc[(df_team_elo['NFL_TEAM'] == team        )&
                                                                                                            (df_team_elo['NFL_YEAR'] == curr_year   )&
                                                                                                            (df_team_elo['NFL_WEEK'].isin([wk,wk+1])),'ELO'].fillna(method='ffill')

                if next_year is not None:
                    ## Take the mean ELO of week 1 of next_year
                    curr_season_mean_elo = df_team_elo.loc[(df_team_elo['NFL_YEAR'] == next_year)&
                                                        (df_team_elo['NFL_WEEK'] == 1        )&
                                                        (df_team_elo['NFL_TEAM'].isin(teams_this_season)),'ELO'].mean()
                    
                    ## Offseason decay -- regress end of year ELO's to the mean and assign to first week of next year
                    df_team_elo.loc[(df_team_elo['NFL_YEAR'] == next_year)&
                                    (df_team_elo['NFL_WEEK'] == 1        )&
                                    (df_team_elo['NFL_TEAM'].isin(teams_this_season)),'ELO'] =  curr_season_mean_elo*(1/3.0) \
                                                                                                + df_team_elo.loc[(df_team_elo['NFL_YEAR'] == next_year)&
                                                                                                                (df_team_elo['NFL_WEEK'] == 1 )&
                                                                                                                (df_team_elo['NFL_TEAM'].isin(teams_this_season)),'ELO']\
                                                                                                *(1-(1/3.0))

            df_team_elo.to_csv(self.ELO_data_file, index=False)
            print(f"> ELO filled data written to file: {self.ELO_data_file}")
            print()

        else:

            ## Read in Wk_by_Wk and Elo data
            df_wk_by_wk = pd.read_csv(self.pfr_cleaned_data_file)
            df_elo      = pd.read_csv(self.ELO_data_file)

            ## Pull out Years and Week numbers
            NFL_YEAR = df_wk_by_wk.loc[df_wk_by_wk.index[-1],'YEAR']
            NFL_WEEK = df_wk_by_wk.loc[df_wk_by_wk.index[-1],'WEEK_NUM']

            print(f"> Generating ELO data for {NFL_YEAR} Week {NFL_WEEK}")

            ## Create a pd.Series of (winner_elo, loser_elo) for each game this week
            elo_series = df_wk_by_wk.loc[(df_wk_by_wk['YEAR']==NFL_YEAR)&(df_wk_by_wk['WEEK_NUM']==NFL_WEEK), ['WINNER','LOSER']].progress_apply(lambda row: elo.update_elo( df_elo.loc[(df_elo['NFL_YEAR']==NFL_YEAR)&
                                                                                                                                                                                        (df_elo['NFL_WEEK']==NFL_WEEK)&
                                                                                                                                                                                        (df_elo['NFL_TEAM']==row.WINNER),'ELO'].values[0],
                                                                                                                                                                             df_elo.loc[(df_elo['NFL_YEAR']==NFL_YEAR)&
                                                                                                                                                                                        (df_elo['NFL_WEEK']==NFL_WEEK)&
                                                                                                                                                                                        (df_elo['NFL_TEAM']==row.LOSER ),'ELO'].values[0], win_or_tie=1 ), axis=1)
            ## Strap together the NFL_TEAM names and the (winner_elo, loser_elo) data
            temp_df = pd.concat([df_wk_by_wk.loc[(df_wk_by_wk['YEAR']==NFL_YEAR    )&
                                                 (df_wk_by_wk['WEEK_NUM']==NFL_WEEK), ['WINNER','LOSER']].reset_index(drop=True),
                                pd.DataFrame(elo_series.tolist()).reset_index(drop=True)],
                                axis=1)

            ## go from 4 columns like ['winner','loser','winner_elo','loser_elo'] to 2 columns ['team','elo']
            temp_df = pd.DataFrame(list(zip(pd.DataFrame(df_wk_by_wk.loc[(df_wk_by_wk['YEAR']==NFL_YEAR)&(df_wk_by_wk['WEEK_NUM']==NFL_WEEK), ['WINNER','LOSER']].reset_index(drop=True).stack())[0].values,
                                            pd.DataFrame(pd.DataFrame(elo_series.tolist()).reset_index(drop=True).stack())[0].values)), columns=['NFL_TEAM','ELO'])
            temp_df['NFL_YEAR'] = NFL_YEAR
            temp_df['NFL_WEEK'] = NFL_WEEK+1
            temp_df = temp_df.loc[:,['NFL_TEAM', 'NFL_YEAR', 'NFL_WEEK','ELO']].sort_values('NFL_TEAM')

            ## Update the newest weeks data with the above calculated elo
            df_elo.loc[(df_elo['NFL_YEAR']==NFL_YEAR)&(df_elo['NFL_WEEK']==NFL_WEEK+1),'ELO'] = temp_df['ELO'].values

            ## Save data
            df_elo.to_csv(self.ELO_data_file, index=False)
            print(f"> ELO filled data written to file: {self.ELO_data_file}")
            print()

    @staticmethod
    def _append_elo_data(self, all_data):

        print('> Adding ELO data to modeling file')
        ## Read in base modeling file and elo data
        base_modeling_file = pd.read_csv(self.modeling_base_file)
        elo_data = pd.read_csv(self.ELO_data_file)

        if not all_data:

            NFL_YEAR = elo_data.loc[~elo_data['ELO'].isnull()].tail(1)['NFL_YEAR'].values[0]
            NFL_WEEK = elo_data.loc[~elo_data['ELO'].isnull()].tail(1)['NFL_WEEK'].values[0]-1

            base_modeling_file = base_modeling_file.loc[(base_modeling_file['YEAR']==NFL_YEAR)&(base_modeling_file['WEEK_NUM']==NFL_WEEK),:]
            elo_data = elo_data.loc[(elo_data['NFL_YEAR']==NFL_YEAR)&(elo_data['NFL_WEEK']==NFL_WEEK),:]

        ## Add Team_A's elo data to base modeling file data
        model_data = pd.merge(base_modeling_file,elo_data, how='left', left_on=['TEAM_A','YEAR','WEEK_NUM'], right_on=['NFL_TEAM','NFL_YEAR','NFL_WEEK'])
        model_data.drop(['NFL_TEAM','NFL_YEAR','NFL_WEEK'], axis=1, inplace=True)
        model_data.columns = ['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM', 'RESULT','TEAM_A_ELO']

        ## Add Team_B's elo data to base modeling file data
        model_data = pd.merge(model_data,elo_data, how='left', left_on=['TEAM_B','YEAR','WEEK_NUM'], right_on=['NFL_TEAM','NFL_YEAR','NFL_WEEK'])
        model_data.drop(['NFL_TEAM','NFL_YEAR','NFL_WEEK'], axis=1, inplace=True)
        model_data.columns = ['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM', 'RESULT','TEAM_A_ELO','TEAM_B_ELO']

        ## just make sure there are no duplicates
        model_data.drop_duplicates(inplace=True)

        if all_data:
            ## ouptput data to file
            model_data.to_csv(self.modeling_file, index=False)
            print(f'> ELO data appended to base_modeling_file: {self.modeling_file}')
            print()
        else:
            ## ouptput data to file
            model_data.to_csv(self.modeling_file, header=False, index=False, mode='a')
            print(f'> ELO data for {NFL_YEAR} week {NFL_WEEK} appended to modeling_file: {self.modeling_file}')
            print()

    def handle_elo_data(self, all_data):

        ## Build Elo features for PFR data
        self._build_PFR_ELO_features(self, all_data)

        ## Append Elo data to base_modeling_file
        self._append_elo_data(self, all_data)


    @staticmethod
    def _append_home_or_away(self, all_data):
        
        print(f'> Building Home or Away feature -- START')
        game_data = pd.read_csv(self.pfr_cleaned_data_file)
        modeling_file = pd.read_csv(self.modeling_file)
        
        if not all_data:
            NFL_YEAR = game_data.tail(1)['YEAR'].values[0]
            NFL_WEEK = game_data.tail(1)['WEEK_NUM'].values[0]

            game_data = game_data.loc[(game_data['YEAR']==NFL_YEAR)&(game_data['WEEK_NUM']==NFL_WEEK),:]

        ## Reduce data to just columns we want to look at here
        game_data = game_data.loc[:,['GAME_ID','WINNER','LOSER','YEAR','WEEK_NUM','GAME_LOCATION']]

        ## Change value of GAME_LOCATION to be 1 for home games or 0 for away games
        game_data['GAME_LOCATION'] = game_data['GAME_LOCATION'].apply(lambda x: 1 if x=='NULL_VALUE' else 0)

        ## Create a 'flipped' version of game_data just swapping the winner and loser column
        game_data_flipped = game_data.loc[:,['GAME_ID','LOSER','WINNER','YEAR','WEEK_NUM','GAME_LOCATION']]
        game_data_flipped['GAME_LOCATION'] =  game_data_flipped['GAME_LOCATION'].apply(lambda x: 0 if x==1 else 1)

        game_data.columns         = ['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM','HOME_FIELD']
        game_data_flipped.columns = ['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM','HOME_FIELD']

        game_data = pd.concat([game_data, game_data_flipped])

        game_data_flipped = None
        del game_data_flipped
        
        if all_data:
            modeling_file = modeling_file.merge(game_data, how='left', on=['GAME_ID','TEAM_A','TEAM_B','YEAR','WEEK_NUM']).drop_duplicates()
            modeling_file.to_csv(self.modeling_file, index=False)
            print(f'> All Home or Away Feature written to {self.modeling_file}')
        else:
            modeling_file.loc[(modeling_file['YEAR']==NFL_YEAR)&(modeling_file['WEEK_NUM']==NFL_WEEK),'HOME_FIELD'] = game_data['HOME_FIELD'].values
            modeling_file.to_csv(self.modeling_file, index=False)
            print(f'> Home or Away Feature for {NFL_YEAR} week {NFL_WEEK} written to {self.modeling_file}')

        print(f'> Building Home or Away feature -- END')
        print()

    @staticmethod
    def _calculate_travel_and_tz_distance(self, all_data=False):
        
        print(f'> Calculating travel and timezone distances -- START')
        df_modeling_file = pd.read_csv(self.modeling_file)
        df_stadium       = pd.read_csv(self.stadium_file)
        df_wk_by_wk      = pd.read_csv(self.pfr_cleaned_data_file)

        if not all_data:
            ## Pull out Years and Week numbers
            NFL_YEAR = df_wk_by_wk.loc[df_wk_by_wk.index[-1],'YEAR']
            NFL_WEEK = df_wk_by_wk.loc[df_wk_by_wk.index[-1],'WEEK_NUM']

            df_wk_by_wk = df_wk_by_wk.loc[  (df_wk_by_wk['YEAR']==NFL_YEAR    )&
                                            (df_wk_by_wk['WEEK_NUM']==NFL_WEEK),:]
            
        ## Create temporary Dataframe to hold ['TEAM_A','TEAM_B','YEAR','WEEK_NUM','HOME_FIELD','LATITUDE_TEAM_A','LONGITUDE_TEAM_A','LATITUDE_TEAM_B','LONGITUDE_TEAM_B']
        #    Used for easily applying distance & tz calculations
        temp_df = pd.merge( df_modeling_file.loc[:,['TEAM_A','TEAM_B','YEAR','WEEK_NUM','HOME_FIELD']],
                            df_stadium.loc[:,['HOME_TEAM','YEAR','LATITUDE','LONGITUDE']]             , 
                            how='left', left_on=['TEAM_A','YEAR'], right_on=['HOME_TEAM','YEAR']).drop('HOME_TEAM',axis=1)
        temp_df = pd.merge( temp_df, 
                            df_stadium.loc[:,['HOME_TEAM','YEAR','LATITUDE','LONGITUDE']],
                            how='left', left_on=['TEAM_B','YEAR'], right_on=['HOME_TEAM','YEAR']).drop('HOME_TEAM',axis=1)
        if not all_data:
            temp_df = temp_df.loc[  (temp_df['YEAR']==NFL_YEAR    )&
                                    (temp_df['WEEK_NUM']==NFL_WEEK),:]
        
        ## Calculate Team A's Kilometers of travel
        temp_df['TEAM_A_KM_TRAVELED'] = temp_df.progress_apply(lambda row: dist_calc._measure_distance( [row['LATITUDE_x'], row['LONGITUDE_x']],
                                                                                                        [row['LATITUDE_y'], row['LONGITUDE_y']]) if row['HOME_FIELD']==0 else 0 , axis=1)
        ## Calculate Team A's Timezone change (US/Pacific -> US/Easter = -300)
        timezone_finder = timezonefinder.TimezoneFinder()
        temp_df['TEAM_A_TZ_TRAVELED'] = temp_df.progress_apply(lambda row: dist_calc._calculate_timezone_difference(    row['LATITUDE_x' ],
                                                                                                                        row['LONGITUDE_x'],
                                                                                                                        row['LATITUDE_y' ],
                                                                                                                        row['LONGITUDE_y'],
                                                                                                                        tz_find = timezone_finder) if row['HOME_FIELD']==0 else 0 , axis=1)
        ## Assign approximate altitude of the stadium the game was played in
        temp_df['ALTITUDE'] = np.nan
        temp_df.loc[temp_df['HOME_FIELD']==1,'ALTITUDE'] = temp_df.loc[temp_df['HOME_FIELD']==1,:].merge(   df_stadium.loc[:,['HOME_TEAM','YEAR','LATITUDE','LONGITUDE','ALTITUDE']], 
                                                                                                            how='left',
                                                                                                            left_on=['TEAM_A','YEAR','LATITUDE_x','LONGITUDE_x'],
                                                                                                            right_on=['HOME_TEAM','YEAR','LATITUDE','LONGITUDE'])['ALTITUDE_y'].values

        temp_df.loc[temp_df['HOME_FIELD']==0,'ALTITUDE'] = temp_df.loc[temp_df['HOME_FIELD']==0,:].merge(   df_stadium.loc[:,['HOME_TEAM','YEAR','LATITUDE','LONGITUDE','ALTITUDE']], 
                                                                                                            how='left',
                                                                                                            left_on=['TEAM_A','YEAR','LATITUDE_x','LONGITUDE_x'],
                                                                                                            right_on=['HOME_TEAM','YEAR','LATITUDE','LONGITUDE'])['ALTITUDE_y'].values


        ## For International games, we need to adjust the Travel distance and timezone difference
        for row in df_wk_by_wk.loc[df_wk_by_wk['STADIUM'].isin(df_stadium.loc[(df_stadium['HOME_TEAM'].isin(['International', 'Alternate'])),'STADIUM']),:].itertuples():
            if ((row.STADIUM == 'Gillette Stadium') & (not row.YEAR =='2002-2003'))|((row.STADIUM == 'TCF Bank Stadium') & (not row.YEAR =='2010-2011'))|((row.STADIUM == 'Husky Stadium') & (not row.YEAR =='1994-1995')):
                continue
            ## Adjust Travel Distance
            temp_df.loc[(((temp_df['TEAM_A']==row.WINNER)&(temp_df['TEAM_B']==row.LOSER ))|
                         ((temp_df['TEAM_A']==row.LOSER) &(temp_df['TEAM_B']==row.WINNER)))&
                        (temp_df['YEAR']==row.YEAR        )&
                        (temp_df['WEEK_NUM']==row.WEEK_NUM),'TEAM_A_KM_TRAVELED'] = temp_df.loc[(((temp_df['TEAM_A']==row.WINNER)&(temp_df['TEAM_B']==row.LOSER  ))|
                                                                                                 ((temp_df['TEAM_A']==row.LOSER) &(temp_df['TEAM_B']==row.WINNER)))&
                                                                                                (temp_df['YEAR']==row.YEAR)&
                                                                                                (temp_df['WEEK_NUM']==row.WEEK_NUM),['LATITUDE_x', 'LONGITUDE_x']].apply(lambda x: dist_calc._measure_distance( [x.LATITUDE_x, x.LONGITUDE_x],
                                                                                                                                                                                                                [df_stadium.loc[(df_stadium['STADIUM']==row.STADIUM)&
                                                                                                                                                                                                                                (df_stadium['YEAR']   ==row.YEAR   ),'LATITUDE'].values[0],
                                                                                                                                                                                                                df_stadium.loc[(df_stadium['STADIUM']==row.STADIUM)&
                                                                                                                                                                                                                                (df_stadium['YEAR']   ==row.YEAR   ),'LONGITUDE'].values[0]]), axis=1)
            ## Adjust Timezone Difference
            temp_df.loc[(((temp_df['TEAM_A']==row.WINNER)&(temp_df['TEAM_B']==row.LOSER  ))|
                         ((temp_df['TEAM_A']==row.LOSER)  &(temp_df['TEAM_B']==row.WINNER)))&
                        (temp_df['YEAR']==row.YEAR)&
                        (temp_df['WEEK_NUM']==row.WEEK_NUM),'TEAM_A_TZ_TRAVELED'] = temp_df.loc[(((temp_df['TEAM_A']==row.WINNER)&(temp_df['TEAM_B']==row.LOSER  ))|
                                                                                                 ((temp_df['TEAM_A']==row.LOSER) &(temp_df['TEAM_B']==row.WINNER)))&
                                                                                                (temp_df['YEAR']==row.YEAR)&
                                                                                                (temp_df['WEEK_NUM']==row.WEEK_NUM),['LATITUDE_x', 'LONGITUDE_x']].apply(lambda x: dist_calc._calculate_timezone_difference(x.LATITUDE_x, 
                                                                                                                                                                                                                            x.LONGITUDE_x,
                                                                                                                                                                                                                            df_stadium.loc[ (df_stadium['STADIUM']==row.STADIUM)&
                                                                                                                                                                                                                                            (df_stadium['YEAR']   ==row.YEAR   ),'LATITUDE'].values[0],
                                                                                                                                                                                                                            df_stadium.loc[ (df_stadium['STADIUM']==row.STADIUM)&
                                                                                                                                                                                                                                            (df_stadium['YEAR']   ==row.YEAR   ),'LONGITUDE'].values[0],
                                                                                                                                                                                                                            tz_find = timezone_finder), axis=1)
            
            ## Adjust Altitude
            temp_df.loc[(((temp_df['TEAM_A']==row.WINNER)&(temp_df['TEAM_B']==row.LOSER  ))|
                         ((temp_df['TEAM_A']==row.LOSER) &(temp_df['TEAM_B']==row.WINNER)))&
                        (temp_df['YEAR']==row.YEAR)&
                        (temp_df['WEEK_NUM']==row.WEEK_NUM),'ALTITUDE'] = df_stadium.loc[   (df_stadium['STADIUM']==row.STADIUM)&
                                                                                            (df_stadium['YEAR']   ==row.YEAR   ),'ALTITUDE'].values[0]

        ## Drop unneeded columns
        # temp_df.drop(['LATITUDE_x', 'LONGITUDE_x', 'LATITUDE_y', 'LONGITUDE_y','HOME_FIELD'],axis=1, inplace=True)
        temp_df = temp_df.rename(columns={'LATITUDE_x'  : 'LATITUDE_A',
                                          'LONGITUDE_x' : 'LONGITUDE_A',
                                          'LATITUDE_y'  : 'LATITUDE_B', 
                                          'LONGITUDE_y' : 'LONGITUDE_B'})
        temp_df.drop(['HOME_FIELD'],axis=1, inplace=True)

        if all_data:
            ## Merge Temp Dataframe back onto modeling_file Dataframe
            df_modeling_file = df_modeling_file.merge(temp_df, how='left', on=['TEAM_A','TEAM_B','YEAR','WEEK_NUM'])

            ## Save data
            df_modeling_file.to_csv(self.modeling_file, index=False)
            print(f"> Travel and Timezone distance written to file: {self.modeling_file}")
            print(f'> Calculating travel and timezone distances -- END')
        
        else:
            ## Set KM/TZ values from temp_df
            df_modeling_file.loc[(df_modeling_file['YEAR']==NFL_YEAR)&(df_modeling_file['WEEK_NUM']==NFL_WEEK),'TEAM_A_KM_TRAVELED'] = temp_df['TEAM_A_KM_TRAVELED'].values
            df_modeling_file.loc[(df_modeling_file['YEAR']==NFL_YEAR)&(df_modeling_file['WEEK_NUM']==NFL_WEEK),'TEAM_A_TZ_TRAVELED'] = temp_df['TEAM_A_TZ_TRAVELED'].values

            ## Save data
            df_modeling_file.to_csv(self.modeling_file)
            print(f"> Travel and Timezone distance written for {NFL_YEAR} week {NFL_WEEK} to file: {self.modeling_file}")
            print(f'> Calculating travel and timezone distances -- END')

    def handle_location_data(self, all_data):
        
        print('> Building Location based features')
        self._append_home_or_away(self, all_data)

        self._calculate_travel_and_tz_distance(self, all_data)