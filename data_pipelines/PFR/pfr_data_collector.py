from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

from PFR_scraper.spiders.NFL_PFR_spider import Historical_PFR_Spider, Upcoming_Schedule_NFLSpider

import os
import re

import pandas as pd
import numpy as np

class PFR_Data_Collector(object):

       def __init__(self):
              ## Data file paths (might move to a config file later, but for now here)
              self.raw_historical_data_file            = 'data/raw/NFL_wk_by_wk.csv'
              self.cleaned_historical_data_file        = 'data/cleaned/NFL_wk_by_wk_cleaned.csv'
              self.raw_upcoming_schedule_data_file     = 'data/raw/NFL_upcoming_schedule.csv'
              self.cleaned_upcoming_schedule_data_file = 'data/cleaned/NFL_upcoming_schedule_cleaned.csv'


       def fetch_historical_data(self):
              settings = Settings()
              os.environ['SCRAPY_SETTINGS_MODULE'] = 'PFR_scraper.settings'
              settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
              settings.setmodule(settings_module_path, priority='project')
              self.process = CrawlerProcess(settings)

              self.process.crawl(Historical_PFR_Spider)
              self.process.start(self)

       def fetch_upcoming_schedule_data(self):
              settings = Settings()
              os.environ['SCRAPY_SETTINGS_MODULE'] = 'PFR_scraper.settings'
              settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
              settings.setmodule(settings_module_path, priority='project')
              self.process = CrawlerProcess(settings)

              self.process.crawl(Upcoming_Schedule_NFLSpider)
              self.process.start(self)

       def clean_upcoming_schedule_data(self):
              print(f'> Reading in PFR Scrapy data from: {self.raw_upcoming_schedule_data_file}')
              ## Read raw Scrapy data pull into DataFrame
              df = pd.read_csv(    self.raw_upcoming_schedule_data_file,
                                   names=['WEEK_NUM','GAME_DAY_OF_WEEK','GAME_DATE',
                                          'AWAY_TEAM','HOME_TEAM','GAMETIME','YEAR'], index_col=False)

              ## In the data pull the header shows up multiple times
              df = df[((df['WEEK_NUM']!='Week')&(df['WEEK_NUM']!='NULL_VALUE')&(~df['WEEK_NUM'].str.contains('Pre')))]

              ## Save cleaned data to 'cleaned_' file
              df.to_csv(self.cleaned_upcoming_schedule_data_file, index=False)
              print(f'> PFR Scrapy data cleaned and written to file: {self.cleaned_upcoming_schedule_data_file}')

       def clean_historical_data(self):

              print(f'> Reading in PFR Scrapy data from: {self.raw_historical_data_file}')
              ## Read raw Scrapy data pull into DataFrame
              df = pd.read_csv(self.raw_historical_data_file,
                            names=['WEEK_NUM','GAME_DAY_OF_WEEK','GAME_DATE','GAMETIME',
                                   'WINNER','GAME_LOCATION','LOSER','BOXSCORE_LINK','PTS_WINNER',
                                   'PTS_LOSER','YARDS_WINNER','TURNOVERS_WINNER','YARDS_LOSER',
                                   'TURNOVERS_LOSER','YEAR','STADIUM','ROOF','SURFACE','VEGAS_LINE','OVER_UNDER',
                                   'TEMPERATURE','HUMIDITY','WIND'], index_col=False)

              ## In the data pull the header shows up multiple times
              df = df[((df['WEEK_NUM']!='Week')&(df['WEEK_NUM']!='NULL_VALUE'))]

              
              df['FAVORITED']  = df['VEGAS_LINE'].apply(lambda x: x[:x.index(' -')] if '-' in x else x)
              df['VEGAS_LINE'] = df['VEGAS_LINE'].apply(lambda x: x[x.index(' -'):] if '-' in x else x)

              ## Replace old team names with the newest version
              df['WINNER'].replace({'St. Louis Rams'           : 'Los Angeles Rams',
                                   'San Diego Chargers'       : 'Los Angeles Chargers',
                                   'Houston Oilers'           : 'Tennessee Titans',
                                   'Tennessee Oilers'         : 'Tennessee Titans',
                                   'Los Angeles Raiders'      : 'Las Vegas Raiders',
                                   'Oakland Raiders'          : 'Las Vegas Raiders',
                                   'Phoenix Cardinals'        : 'Arizona Cardinals',
                                   'Washington Redskins'      : 'Washington Commanders',
                                   'Washington Football Team' : 'Washington Commanders'},inplace=True)
              df['LOSER'].replace({ 'St. Louis Rams'           : 'Los Angeles Rams',
                                   'San Diego Chargers'       : 'Los Angeles Chargers',
                                   'Houston Oilers'           : 'Tennessee Titans',
                                   'Tennessee Oilers'         : 'Tennessee Titans',
                                   'Los Angeles Raiders'      : 'Las Vegas Raiders',
                                   'Oakland Raiders'          : 'Las Vegas Raiders',
                                   'Phoenix Cardinals'        : 'Arizona Cardinals',
                                   'Washington Redskins'      : 'Washington Commanders',
                                   'Washington Football Team' : 'Washington Commanders'},inplace=True)
              
              df['FAVORITED'].replace({ 'St. Louis Rams'           : 'Los Angeles Rams',
                                   'San Diego Chargers'       : 'Los Angeles Chargers',
                                   'Houston Oilers'           : 'Tennessee Titans',
                                   'Tennessee Oilers'         : 'Tennessee Titans',
                                   'Los Angeles Raiders'      : 'Las Vegas Raiders',
                                   'Oakland Raiders'          : 'Las Vegas Raiders',
                                   'Phoenix Cardinals'        : 'Arizona Cardinals',
                                   'Washington Redskins'      : 'Washington Commanders',
                                   'Washington Football Team' : 'Washington Commanders'},inplace=True)

              ## Data pull error, easier to fix here
              if '01-02' in df['YEAR'].unique():
                     df['YEAR'].replace({ "99-100":'99-00',
                                          '00-1'  :'00-01',
                                          '01-2'  :'01-02',
                                          '02-3'  :'02-03',
                                          '03-4'  :'03-04',
                                          '04-5'  :'04-05',
                                          '05-6'  :'05-06',
                                          '06-7'  :'06-07',
                                          '07-8'  :'07-08',
                                          '08-9'  :'08-09'}, inplace=True)
              elif '2-Jan' in df['YEAR'].unique(): ## Incase you open the CSV in Excel, it auto-formats oddly
                     df['YEAR'].replace({ "99-100":'99-00',
                                          '00-1'  :'00-01',
                                          '2-Jan' :'01-02',
                                          '3-Feb' :'02-03',
                                          '4-Mar' :'03-04',
                                          '5-Apr' :'04-05',
                                          '6-May' :'05-06',
                                          '7-Jun' :'06-07',
                                          '8-Jul' :'07-08',
                                          '9-Aug' :'08-09',
                                          '10-Sep':'09-10',
                                          '11-Oct':'10-11',
                                          '12-Nov':'11-12',
                                          '13-Dec':'12-13'}, inplace=True)

              ## Game Date to datetime
              df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

              ## Create sorted year list like '91-92' to use as categorical ordering list
              first_year = sorted(df['GAME_DATE'].dt.year.unique())[0]
              last_year  = sorted(df['GAME_DATE'].dt.year.unique())[-1]
              sorted_years = [f"{str(yr)[-2:]}"+"-"+f"{str(yr+1)[-2:]}" for yr in range(int(first_year), int(last_year)+1) ]
              increase_in_number_of_weeks_in_season = sorted_years.index('21-22')

              ## Create lists of year numbers to denote different length seasons
              seasons_with_21_weeks = sorted_years[:increase_in_number_of_weeks_in_season]
              seasons_with_21_weeks.remove('93-94') ## This year had an extra bye week
              seasons_with_22_weeks = sorted_years[increase_in_number_of_weeks_in_season:]
              seasons_with_22_weeks.append('93-94')

              ## Change playoff week names to numbers
              ## prior to '21-22' season
              df.loc[df['YEAR'].isin(seasons_with_21_weeks),:] = df.loc[df['YEAR'].isin(seasons_with_21_weeks),:].replace({'WildCard'  : '18',
                                                                                                                       'Division'  : '19',
                                                                                                                       'ConfChamp' : '20',
                                                                                                                       'SuperBowl' : '21'})
              ## after to '21-22' season
              df.loc[df['YEAR'].isin(seasons_with_22_weeks),:] = df.loc[df['YEAR'].isin(seasons_with_22_weeks),:].replace({'WildCard'  : '19',
                                                                                                                       'Division'  : '20',
                                                                                                                       'ConfChamp' : '21',
                                                                                                                       'SuperBowl' : '22'})

              ## These data points are games were teams tied, the data didn't pull because of a "strong" tag
              df.loc[df['PTS_WINNER']=='NULL_VALUE','PTS_WINNER'] = df.loc[df['PTS_WINNER']=='NULL_VALUE','PTS_LOSER']

              ## Reduce Temp, Humidity, Wind down to just numbers
              df['TEMPERATURE'] = df['TEMPERATURE'].str.extract('(\d+)')
              df['HUMIDITY'] = df['HUMIDITY'].str.extract('(\d+)')
              df['WIND'] = df['WIND'].str.extract('(\d+)')

              df['TEMPERATURE'] = df['TEMPERATURE'].replace('',np.nan)
              df['HUMIDITY']    = df['HUMIDITY'].replace('',np.nan)
              df['WIND']        = df['WIND'].replace('',np.nan)
              df['VEGAS_LINE']  = df['VEGAS_LINE'].replace('Pick',np.nan)
              df['FAVORITED']   = df['FAVORITED'].replace('Pick',np.nan)

              ## Change values to be numeric
              df['WEEK_NUM']         = df['WEEK_NUM'].astype('int')
              df['PTS_WINNER']       = df['PTS_WINNER'].astype('int')
              df['PTS_LOSER']        = df['PTS_LOSER'].astype('int')
              df['YARDS_WINNER']     = df['YARDS_WINNER'].astype('int')
              df['TURNOVERS_WINNER'] = df['TURNOVERS_WINNER'].astype('int')
              df['YARDS_LOSER']      = df['YARDS_LOSER'].astype('int')
              df['TURNOVERS_LOSER']  = df['TURNOVERS_LOSER'].astype('int')
              df['OVER_UNDER']       = df['OVER_UNDER'].astype('float')
              df['VEGAS_LINE']       = df['VEGAS_LINE'].astype('float')
              df['TEMPERATURE']      = df['TEMPERATURE'].astype('float')
              df['HUMIDITY']         = df['HUMIDITY'].astype('float')
              df['WIND']             = df['WIND'].astype('float')

              ## Create a column that denotes a win or a tie (could be used for prediction measuring)
              df.loc[df['PTS_WINNER']>df['PTS_LOSER'],'WIN_OR_TIE'] = 1
              df['WIN_OR_TIE'].fillna(0,inplace=True)

              ## Since the web scraper doesn't want to correctly pull the tie info, I am manually going to set the ties.  

              ## 1997-11-16 Ravens v Eagles
              df.loc[(df['WINNER']    == 'Baltimore Ravens')&
                     (df['LOSER']     == 'Philadelphia Eagles')&
                     (df['GAME_DATE'] == '1997-11-16'), ['PTS_WINNER','PTS_LOSER']] = 10
              
              ## 1997-11-23 Giants v Washington
              df.loc[(df['WINNER']    == 'Washington Commanders')&
                     (df['LOSER']     == 'New York Giants')&
                     (df['GAME_DATE'] == '1997-11-23'), ['PTS_WINNER','PTS_LOSER']] = 7
              
              ## 2002-11-10 Falcons v Steelers
              df.loc[(df['WINNER']    == 'Pittsburgh Steelers')&
                     (df['LOSER']     == 'Atlanta Falcons')&
                     (df['GAME_DATE'] == '2002-11-10'), ['PTS_WINNER','PTS_LOSER']] = 34
              
              ## 2008-11-16 Eagles v Bengals
              df.loc[(df['WINNER']    == 'Philadelphia Eagles')&
                     (df['LOSER']     == 'Cincinnati Bengals')&
                     (df['GAME_DATE'] == '2008-11-16'), ['PTS_WINNER','PTS_LOSER']] = 13
              
              ## 2012-11-11 Rams v 49ers
              df.loc[(df['WINNER']    == 'San Francisco 49ers')&
                     (df['LOSER']     == 'Los Angeles Rams')&
                     (df['GAME_DATE'] == '2012-11-11'), ['PTS_WINNER','PTS_LOSER']] = 24
              
              ## 2013-11-24 Vikings v Packers
              df.loc[(df['WINNER']    == 'Minnesota Vikings')&
                     (df['LOSER']     == 'Green Bay Packers')&
                     (df['GAME_DATE'] == '2013-11-24'), ['PTS_WINNER','PTS_LOSER']] = 26
              
              ## 2014-10-12 Panthers v Bengals
              df.loc[(df['WINNER']    == 'Cincinnati Bengals')&
                     (df['LOSER']     == 'Carolina Panthers')&
                     (df['GAME_DATE'] == '2014-10-12'), ['PTS_WINNER','PTS_LOSER']] = 37
              
              ## 2016-10-23 Seahawks v Cardinals
              df.loc[(df['WINNER']    == 'Seattle Seahawks')&
                     (df['LOSER']     == 'Arizona Cardinals')&
                     (df['GAME_DATE'] == '2016-10-23'), ['PTS_WINNER','PTS_LOSER']] = 6
              
              ## 2016-10-30 Washington v Bengals
              df.loc[(df['WINNER']    == 'Washington Commanders')&
                     (df['LOSER']     == 'Cincinnati Bengals')&
                     (df['GAME_DATE'] == '2016-10-30'), ['PTS_WINNER','PTS_LOSER']] = 27
              
              ## 2018-09-09 Steelers v Browns
              df.loc[(df['WINNER']    == 'Pittsburgh Steelers')&
                     (df['LOSER']     == 'Cleveland Browns')&
                     (df['GAME_DATE'] == '2018-09-09'), ['PTS_WINNER','PTS_LOSER']] = 21
              
              ## 2018-09-16 Vikings v Packers
              df.loc[(df['WINNER']    == 'Minnesota Vikings')&
                     (df['LOSER']     == 'Green Bay Packers')&
                     (df['GAME_DATE'] == '2018-09-16'), ['PTS_WINNER','PTS_LOSER']] = 29
              
              ## 2019-09-08 Lions v Cardinals
              df.loc[(df['WINNER']    == 'Detroit Lions')&
                     (df['LOSER']     == 'Arizona Cardinals')&
                     (df['GAME_DATE'] == '2019-09-08'), ['PTS_WINNER','PTS_LOSER']] = 27
              
              ## 2020-09-27 Bengals v Eagles
              df.loc[(df['WINNER']    == 'Philadelphia Eagles')&
                     (df['LOSER']     == 'Cincinnati Bengals')&
                     (df['GAME_DATE'] == '2020-09-27'), ['PTS_WINNER','PTS_LOSER']] = 23
              
              ## 2021-11-14 Lions v Steelers
              df.loc[(df['WINNER']    == 'Pittsburgh Steelers')&
                     (df['LOSER']     == 'Detroit Lions')&
                     (df['GAME_DATE'] == '2021-11-14'), ['PTS_WINNER','PTS_LOSER']] = 16
              
              ## 2022-09-11 Colts v Texans
              df.loc[(df['WINNER']    == 'Houston Texans')&
                     (df['LOSER']     == 'Indianapolis Colts')&
                     (df['GAME_DATE'] == '2022-09-11'), ['PTS_WINNER','PTS_LOSER']] = 20
              
              ## 2022-12-04 Washington v Giants
              df.loc[(df['WINNER']    == 'Washington Commanders')&
                     (df['LOSER']     == 'New York Giants')&
                     (df['GAME_DATE'] == '2022-12-04'), ['PTS_WINNER','PTS_LOSER']] = 20
              
              ## Set 'WIN_OR_TIE' to .5 to denote a tie
              df.loc[df['PTS_WINNER']==df['PTS_LOSER'], 'WIN_OR_TIE'] = 0.5

              df.sort_values(['GAME_DATE','GAMETIME'], ascending=True, inplace=True)

              ## Save cleaned data to 'cleaned_' file
              df.to_csv(self.cleaned_historical_data_file, index=False)
              print(f'> PFR Scrapy data cleaned and written to file: {self.cleaned_historical_data_file}')
       