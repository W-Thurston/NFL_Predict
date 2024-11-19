import pandas as pd
import numpy as np

from tqdm import tqdm

import utils.elo as elo

class pfr_model_builder(object):

    def __init__(self):
        
        self.ELO_data_file               = 'data/cleaned/NFL_Team_Elo.csv'
        self.upcoming_schedule_filename  = 'data/cleaned/NFL_upcoming_schedule_cleaned.csv'
        self.ELO_visualization_file      = 'data/output/Ranks_and_Betting.xlsx'
        self.cleaned_data_file           = 'data/cleaned/NFL_wk_by_wk_cleaned.csv'

    def predict_elo_only(self, year: str = '2023-2024', week: int = 1):
        
        print(f"> Reading in upcoming schedule & Elo data from: \n\t{self.upcoming_schedule_filename}\n\t{self.ELO_data_file}")
        df_elo      = pd.read_csv(self.ELO_data_file)
        df_schedule = pd.read_csv(self.upcoming_schedule_filename)
        df_schedule = df_schedule.loc[(df_schedule['YEAR']==year)&(df_schedule['WEEK_NUM']==week), :]

        ## Merge the Elo data onto the schedule for both Away and Home teams
        df_schedule = pd.merge(df_schedule, df_elo, how='left', left_on=['AWAY_TEAM', 'YEAR', 'WEEK_NUM'], right_on=['NFL_TEAM', 'NFL_YEAR', 'NFL_WEEK']).drop(['NFL_TEAM', 'NFL_YEAR', 'NFL_WEEK'],axis=1).rename(columns={'ELO':'AWAY_TEAM_ELO'})
        df_schedule = pd.merge(df_schedule, df_elo, how='left', left_on=['HOME_TEAM', 'YEAR', 'WEEK_NUM'], right_on=['NFL_TEAM', 'NFL_YEAR', 'NFL_WEEK']).drop(['NFL_TEAM', 'NFL_YEAR', 'NFL_WEEK'],axis=1).rename(columns={'ELO':'HOME_TEAM_ELO'})

        ## Clean up after yourself
        df_elo = None

        ## Drop all NaNs, which in this case are weeks that won't have Elo's just yet
        df_schedule.dropna(inplace=True)

        ## Calculate Win Probability for both Home and Away teams
        df_schedule[['AWAY_TEAM_WIN_PROB','HOME_TEAM_WIN_PROB']] = df_schedule.apply(lambda x: elo.elo_win_probability(x.AWAY_TEAM_ELO, x.HOME_TEAM_ELO), axis=1, result_type='expand')
        
        ## Change Probability to be more readable
        df_schedule['AWAY_TEAM_WIN_PROB'] = df_schedule['AWAY_TEAM_WIN_PROB'].apply(lambda x: f'{x*100:.1f} %')
        df_schedule['HOME_TEAM_WIN_PROB'] = df_schedule['HOME_TEAM_WIN_PROB'].apply(lambda x: f'{x*100:.1f} %')

        ## Drop unneeded columns
        df_schedule.drop(['YEAR'],axis=1, inplace=True)

        with pd.ExcelWriter(self.ELO_visualization_file, mode='a', if_sheet_exists='overlay') as writer:
            df_schedule.to_excel(   excel_writer=writer,
                                    sheet_name='Upcoming Weeks Predictions',
                                    index=False,
                                    header=False, 
                                    startrow=2,
                                    startcol=14)
            
        print(f"> Saving Elo based predictions to: {self.ELO_visualization_file}")

    def evaluate_elo_only(self, time_period:str='YEAR', ranking_system=elo.elo_win_probability):

        df     = pd.read_csv(self.cleaned_data_file)
        df     = df.loc[:,['WEEK_NUM','WINNER','LOSER','YEAR','WIN_OR_TIE']]

        df_elo = pd.read_csv(self.ELO_data_file)
        elo_prob = []

        for row in tqdm(df.itertuples(), total=df.shape[0]):
            
            winning_team_name = getattr(row,'WINNER')
            losing_team_name  = getattr(row,'LOSER')

            YEAR = getattr(row,'YEAR')
            WEEK = getattr(row,'WEEK_NUM')
            
            elo_prob.append(ranking_system( df_elo.loc[(df_elo['NFL_TEAM'] == winning_team_name)&
                                                       (df_elo['NFL_YEAR'] == YEAR             )&
                                                       (df_elo['NFL_WEEK'] == WEEK             ),'ELO'].values[0],
                                            df_elo.loc[(df_elo['NFL_TEAM'] == losing_team_name )&
                                                       (df_elo['NFL_YEAR'] == YEAR             )&
                                                       (df_elo['NFL_WEEK'] == WEEK             ),'ELO'].values[0])[0] )
        
        df['ELO_PROB'] = elo_prob

        df.loc[(df['WIN_OR_TIE']==1)&(df['ELO_PROB']>.5),'CORRECT'] = 1
        df['CORRECT'] = df['CORRECT'].fillna(0)

        if time_period == 'YEAR':
            for i in df['YEAR'].unique():
                print(f"{i}: {(df.loc[df['YEAR']==i,'CORRECT'].sum()/df.loc[df['YEAR']==i,:].shape[0]):.0%} correct on the season")
        elif time_period == 'WEEK':
            for i in df['WEEK_NUM'].unique():
                print(f"{i:02}: {(df.loc[df['WEEK_NUM']==i,'CORRECT'].sum()/df.loc[df['WEEK_NUM']==i,:].shape[0]):.0%} correct for this week in season")

        print(f"Overall: {(df['CORRECT'].sum()/df.shape[0]):.0%}")
        print()

    def build_new_ranks(self, year:str, week:int):
        '''
        Used to visualize changes in ELO ranking from before the current week's Thursday night game to after the current week's Monday night game

        year: str -> Current Year you'd like to visualize (ex "2023-2024")
        week: int -> Current Week you'd like to visualize (ex 1) 

        '''
        print(f"> Adjusting elo ranks based on {year} and {week}")
        df_elo = pd.read_csv(self.ELO_data_file)

        ## Reduce RAM usage
        df_elo = df_elo.loc[(df_elo['NFL_YEAR']==year)&
                            (df_elo['NFL_WEEK'].isin([week,week+1])),:]
        
        df1 = df_elo.loc[(df_elo['NFL_YEAR']==year)&(df_elo['NFL_WEEK']==week  ),['NFL_TEAM','ELO']].sort_values(['ELO'],ascending=False).reset_index().drop('index',axis=1)
        df1['Rank'] = np.arange(1,df1.shape[0]+1)
        df1['EMPTY'] = np.nan
        df2 = df_elo.loc[(df_elo['NFL_YEAR']==year)&(df_elo['NFL_WEEK']==week+1),['NFL_TEAM','ELO']].sort_values(['ELO'],ascending=False).reset_index().drop('index',axis=1)
        df2['Rank'] = np.arange(1,df2.shape[0]+1)

        df3 = pd.concat([df1, df2],axis=1)

        with pd.ExcelWriter(self.ELO_visualization_file, mode='a', if_sheet_exists='overlay') as writer:
            df3.to_excel(   excel_writer= writer,
                            sheet_name = 'ELO Ranking Changes',
                            index=False,
                            header=False, 
                            startrow = 3,
                            startcol = 14)
        
        print(f"> Saving updated Elo Ranks to: {self.ELO_visualization_file}")
            
    def play_out_rest_of_season(self, year:str, week:int):
        '''
        Used to simulate the rest of the season based on inputed year and week.

        '''
        
        ## Read in Elo and upcoming schedule data
        df_elo      = pd.read_csv(self.ELO_data_file)
        df_schedule = pd.read_csv(self.upcoming_schedule_filename)

        
