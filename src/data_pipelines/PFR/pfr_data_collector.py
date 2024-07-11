from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

import pandas as pd
import numpy as np

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datetime import datetime
import timezonefinder, pytz

from src.PFR_scraper.spiders.NFL_PFR_spider import Historical_PFR_Spider, Append_New_PFR_Spider, Upcoming_Schedule_NFLSpider
from utils.stadium_loc_dist_calc import _convert

class PFR_Data_Collector(object):

       def __init__(self):
              ## Data file paths (might move to a config file later, but for now here)
              self.raw_historical_data_file            = 'data/raw/NFL_wk_by_wk.csv'
              self.cleaned_historical_data_file        = 'data/cleaned/NFL_wk_by_wk_cleaned.csv'
              self.raw_upcoming_schedule_data_file     = 'data/raw/NFL_upcoming_schedule.csv'
              self.cleaned_upcoming_schedule_data_file = 'data/cleaned/NFL_upcoming_schedule_cleaned.csv'
              self.ELO_visualization_file              = 'data/output/Ranks_and_Betting.xlsx'
              self.long_to_short_team_name_file        = 'data/cleaned/NFL_long_to_short_name.csv'
              self.stadium_file                        = 'data/cleaned/NFL_stadium_reference.csv'
              self.weather_file                        = 'data/cleaned/NFL_wk_by_wk_w_weather.csv'


       def fetch_historical_data(self, all_data:bool = False, scrape_year:str = '2023'):
              print(f"{all_data} || {scrape_year}\n\n\n")
              if all_data:
                     settings = Settings()
                     os.environ['SCRAPY_SETTINGS_MODULE'] = 'src.PFR_scraper.settings'
                     settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
                     settings.setmodule(settings_module_path, priority='project')
                     self.process = CrawlerProcess(settings)
                     self.process.crawl(Historical_PFR_Spider)
                     self.process.start(self)
              else:
                     settings = Settings()
                     os.environ['SCRAPY_SETTINGS_MODULE'] = 'src.PFR_scraper.settings'
                     settings_module_path = os.environ['SCRAPY_SETTINGS_MODULE']
                     settings.setmodule(settings_module_path, priority='project')
                     self.process = CrawlerProcess(settings)
                     self.process.crawl(Append_New_PFR_Spider, year=scrape_year)
                     self.process.start(self)

       def fetch_upcoming_schedule_data(self):
              settings = Settings()
              os.environ['SCRAPY_SETTINGS_MODULE'] = 'src.PFR_scraper.settings'
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
              df = pd.read_csv(    self.raw_historical_data_file,
                                   names=['WEEK_NUM','GAME_DAY_OF_WEEK','GAME_DATE','GAMETIME',
                                          'WINNER','GAME_LOCATION','LOSER','BOXSCORE_LINK','PTS_WINNER',
                                          'PTS_LOSER','YARDS_WINNER','TURNOVERS_WINNER','YARDS_LOSER',
                                          'TURNOVERS_LOSER','YEAR','STADIUM','ROOF','SURFACE','VEGAS_LINE','OVER_UNDER'], index_col=False)

              ## In the data pull the header shows up multiple times
              df = df[((df['WEEK_NUM']!='Week')&(df['WEEK_NUM']!='NULL_VALUE'))]

              ## Drop duplicate values; duplicates determined across all columns
              df.drop_duplicates(['WEEK_NUM','GAME_DAY_OF_WEEK','GAME_DATE',
                                  'GAMETIME','WINNER','GAME_LOCATION','LOSER'], inplace=True)

              ## Split the pulled value of 'VEGAS_LINE' into Favorite (NFL team name) and Vegas Score line (how much they are favorited by)
              df['FAVORITED']  = df['VEGAS_LINE'].apply(lambda x: x[:x.index(' -')] if '-' in x else x)
              df['VEGAS_LINE'] = df['VEGAS_LINE'].apply(lambda x: x[x.index(' -'):] if '-' in x else x)

              ## Replace old team names with the newest version
              df['WINNER'] = df['WINNER'].replace({       'St. Louis Rams'           : 'Los Angeles Rams',
                                                          'San Diego Chargers'       : 'Los Angeles Chargers',
                                                          'Houston Oilers'           : 'Tennessee Titans',
                                                          'Tennessee Oilers'         : 'Tennessee Titans',
                                                          'Los Angeles Raiders'      : 'Las Vegas Raiders',
                                                          'Oakland Raiders'          : 'Las Vegas Raiders',
                                                          'Phoenix Cardinals'        : 'Arizona Cardinals',
                                                          'Washington Redskins'      : 'Washington Commanders',
                                                          'Washington Football Team' : 'Washington Commanders'})
              df['LOSER'] = df['LOSER'].replace({         'St. Louis Rams'           : 'Los Angeles Rams',
                                                          'San Diego Chargers'       : 'Los Angeles Chargers',
                                                          'Houston Oilers'           : 'Tennessee Titans',
                                                          'Tennessee Oilers'         : 'Tennessee Titans',
                                                          'Los Angeles Raiders'      : 'Las Vegas Raiders',
                                                          'Oakland Raiders'          : 'Las Vegas Raiders',
                                                          'Phoenix Cardinals'        : 'Arizona Cardinals',
                                                          'Washington Redskins'      : 'Washington Commanders',
                                                          'Washington Football Team' : 'Washington Commanders'})
              df['FAVORITED'] = df['FAVORITED'].replace({ 'St. Louis Rams'           : 'Los Angeles Rams',
                                                          'San Diego Chargers'       : 'Los Angeles Chargers',
                                                          'Houston Oilers'           : 'Tennessee Titans',
                                                          'Tennessee Oilers'         : 'Tennessee Titans',
                                                          'Los Angeles Raiders'      : 'Las Vegas Raiders',
                                                          'Oakland Raiders'          : 'Las Vegas Raiders',
                                                          'Phoenix Cardinals'        : 'Arizona Cardinals',
                                                          'Washington Redskins'      : 'Washington Commanders',
                                                          'Washington Football Team' : 'Washington Commanders'})

              ## Game Date to datetime
              try:
                     df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], format="mixed", dayfirst=False, yearfirst=True)
                     # df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], format="%Y-%m-%d", dayfirst=False, yearfirst=True)
              except:
                     df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], format="%m/%d/%Y", dayfirst=False, yearfirst=False)

              ## Overwrite Year column
              df['YEAR'] = df['GAME_DATE'].apply(lambda x: f"{x.year}-{x.year+1}" if x.month in [8,9,10,11,12] else f"{x.year-1}-{x.year}")
              
              ## Create sorted year list like '1991-1992'
              sorted_years = sorted(df['YEAR'].unique())
              df['YEAR'] = pd.Categorical(df['YEAR'], sorted_years)
              
              increase_in_number_of_weeks_in_season = sorted_years.index('2021-2022')

              ## Create lists of year numbers to denote different length seasons
              seasons_with_21_weeks = sorted_years[:increase_in_number_of_weeks_in_season]
              seasons_with_21_weeks.remove('1993-1994') ## This year had an extra bye week
              seasons_with_22_weeks = sorted_years[increase_in_number_of_weeks_in_season:]
              seasons_with_22_weeks.append('1993-1994') ## This year had an extra bye week

              ## Change playoff week names to numbers
              ## prior to '21-22' season
              df.loc[df['YEAR'].isin(seasons_with_21_weeks),:] = df.loc[df['YEAR'].isin(seasons_with_21_weeks),:].replace({   'WildCard'  : '18',
                                                                                                                              'Division'  : '19',
                                                                                                                              'ConfChamp' : '20',
                                                                                                                              'SuperBowl' : '21'})
              ## after to '21-22' season
              df.loc[df['YEAR'].isin(seasons_with_22_weeks),:] = df.loc[df['YEAR'].isin(seasons_with_22_weeks),:].replace({   'WildCard'  : '19',
                                                                                                                              'Division'  : '20',
                                                                                                                              'ConfChamp' : '21',
                                                                                                                              'SuperBowl' : '22'})

              ## These data points are games were teams tied, the data didn't pull because of a "strong" tag
              df.loc[df['PTS_WINNER']=='NULL_VALUE','PTS_WINNER'] = df.loc[df['PTS_WINNER']=='NULL_VALUE','PTS_LOSER']

              df['VEGAS_LINE']  = df['VEGAS_LINE'].replace('Pick',np.nan)
              df['FAVORITED']   = df['FAVORITED'].replace('Pick',np.nan)
              df['OVER_UNDER']  = df['OVER_UNDER'].replace('NULL_VALUE',np.nan)

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

              ## Create a column that denotes a win or a tie (could be used for prediction measuring)
              df.loc[df['PTS_WINNER']>df['PTS_LOSER'],'WIN_OR_TIE'] = 1
              df['WIN_OR_TIE'] = df['WIN_OR_TIE'].fillna(0)

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

              ## Create a GAME_ID column
              # create series of "YYYY-Wk-Away_team-Home_team" to use as an ID column
              temp_id = df.apply(lambda row:  f"{row['YEAR'][:4]}_{row['WEEK_NUM']:02}_{row['WINNER'] if row['GAME_LOCATION']=='@' else (row['WINNER'] if row['GAME_LOCATION']=='N' else row['LOSER'])}_{row['WINNER'] if row['GAME_LOCATION']=='NULL_VALUE' else row['LOSER']}" ,axis=1)
              temp_id = pd.DataFrame(temp_id, columns=['LONG_ID'])
              
              # Read in data that maps NFL's Long team name (ex: Arizona Cardinals) to NFL's short team name (ex: ARI)
              df_short = pd.read_csv(self.long_to_short_team_name_file)

              # Create dictionary to use for pandas series mapping
              df_short_dict = {}
              for row in df_short.itertuples():
                     df_short_dict[row.NFL_LONG_NAME] = row.NFL_SHORT_NAME

              # Map Long names to short names
              temp_id = temp_id['LONG_ID'].replace(df_short_dict, regex=True)
              df['GAME_ID'] = temp_id

              # Clean up after yourself
              df_short, df_short_dict, temp_id = None, None, None
              del df_short, df_short_dict, temp_id

              df['STADIUM'] = df['STADIUM'].replace({'Dolphin Stadium':'Dolphins Stadium'})

              ## Sort wk_by_wk file
              df.sort_values(['GAME_DATE','GAMETIME', 'GAME_ID'], ascending=True, inplace=True, ignore_index=True)

              ## Set ordering of columns
              df = df.loc[:,[      'GAME_ID', 'WEEK_NUM', 'GAME_DAY_OF_WEEK', 'GAME_DATE', 'GAMETIME', 
                                   'WINNER', 'GAME_LOCATION', 'LOSER', 'BOXSCORE_LINK', 'PTS_WINNER', 
                                   'PTS_LOSER', 'YARDS_WINNER', 'TURNOVERS_WINNER', 'YARDS_LOSER', 
                                   'TURNOVERS_LOSER', 'YEAR', 'STADIUM', 'ROOF', 'SURFACE', 'VEGAS_LINE', 
                                   'OVER_UNDER', 'FAVORITED', 'WIN_OR_TIE']]

              ## Save cleaned data to 'cleaned_' file
              df.to_csv(self.cleaned_historical_data_file, index=False)
              print(f'> PFR Scrapy data cleaned and written to file: {self.cleaned_historical_data_file}')
       
       @staticmethod
       def _convert_12hour_to_24hour( time_12hour):
              in_time  = datetime.strptime(time_12hour, "%I:%M%p" )
              out_time = datetime.strftime(in_time,     "%H:%M:%S")

              return out_time

       @staticmethod
       def _isTimeFormat(input, format):
              try:
                     datetime.strptime(input, format)
                     return True
              except ValueError:
                     return False 

       @staticmethod
       def _pull_openWeatherMap_data(self, row, tf, session, OWM_API_KEY):
              '''
                     Pull data from Open Wheater Map
              '''
              try: 
                     ## Make sure Lat & Lon are in decimal format
                     lat      = _convert(row.LATITUDE)
                     lon      = _convert(row.LONGITUDE)
                     
                     ## Build a timestamp that is accepted by the API
                     local    = pytz.timezone(tf.certain_timezone_at(lat=lat, lng=lon))
                     date = f'{row.GAME_DATE.year}-{row.GAME_DATE.month}-{row.GAME_DATE.day}'

                     if self._isTimeFormat(f"{date} {self._convert_12hour_to_24hour(row.GAMETIME)}", "%Y-%m-%d %H:%M:%S"):
                            naive    = datetime.strptime(f"{date} {self._convert_12hour_to_24hour(row.GAMETIME)}", "%Y-%m-%d %H:%M:%S")
                     elif self._isTimeFormat(f"{date} {self._convert_12hour_to_24hour(row.GAMETIME)}", "%m-%d-%Y %H:%M:%S"):
                            naive    = datetime.strptime(f"{date} {self._convert_12hour_to_24hour(row.GAMETIME)}", "%m-%d-%Y %H:%M:%S")
                     else:
                            naive    = datetime.strptime(f"{date} {self._convert_12hour_to_24hour(row.GAMETIME)}", "%m/%d/%Y %H:%M:%S")

                     local_dt = local.localize(naive, is_dst=None)
                     utc_dt   = local_dt.astimezone(pytz.utc)
                     time     = int(utc_dt.timestamp())

                     ## Build the API's URL
                     url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={time}&appid={OWM_API_KEY}"

                     ## Pull API's response as JSON
                     owm_response = session.get(url).json()

                     ## Gather wanted data fields from JSON response
                     row['TEMP']         = owm_response['data'][0].get('temp',       'NULL_VALUE')
                     row['FEELS_LIKE']   = owm_response['data'][0].get('feels_like', 'NULL_VALUE')
                     row['PRESSURE']     = owm_response['data'][0].get('pressure',   'NULL_VALUE')
                     row['HUMIDITY']     = owm_response['data'][0].get('humidity',   'NULL_VALUE')
                     row['DEW_POINT']    = owm_response['data'][0].get('dew_point',  'NULL_VALUE')
                     row['CLOUDS']       = owm_response['data'][0].get('clouds',     'NULL_VALUE')
                     row['VISIBILITY']   = owm_response['data'][0].get('visibility', 'NULL_VALUE')
                     row['WIND_SPEED']   = owm_response['data'][0].get('wind_speed', 'NULL_VALUE')
                     row['WIND_DEG']     = owm_response['data'][0].get('wind_deg',   'NULL_VALUE')
                     row['WEATHER_MAIN'] = owm_response['data'][0]['weather'][0].get('main',        'NULL_VALUE')
                     row['WEATHER_DESC'] = owm_response['data'][0]['weather'][0].get('description', 'NULL_VALUE')

                     return row
              except:
                     print(f"ID: {row.GAME_ID}")


       def pull_weather_data(self, year ,OWM_API_KEY):

              ## Read in wk_by_wk data
              df = pd.read_csv(self.cleaned_historical_data_file)
              df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
              df.sort_values(['GAME_DATE','GAMETIME', 'GAME_ID'], ascending=True, inplace=True)
              
              ## Read in stadium data 
              df_stadium = pd.read_csv(self.stadium_file)

              ## Merge Stadium Latitude and Longitude on to wk_by_wk data
              temp_df = df.loc[:,['GAME_ID','GAME_DATE','GAMETIME', 'YEAR','STADIUM']].copy()
              temp_df = temp_df.merge(df_stadium.loc[:,['YEAR','STADIUM','LATITUDE', 'LONGITUDE']],how='left', on=['YEAR','STADIUM']).drop_duplicates()
              temp_df.sort_values(['GAME_DATE','GAMETIME', 'GAME_ID'], ascending=True, inplace=True, ignore_index=True)
              
              ## Reduce temp_df down to just the new week's worth of data
              temp_df = temp_df.iloc[df.loc[(df['YEAR']==year)&(df['WEEK_NUM']==df.iloc[-1,:]['WEEK_NUM']),:].index,:]

              ## Create a TimezoneFinder object
              tzf = timezonefinder.TimezoneFinder()

              ## Set up the Requests.Session for pulling data from openweathermap
              sess = requests.Session()
              retry = Retry(connect=3, backoff_factor=0.5)
              adapter = HTTPAdapter(max_retries=retry)
              sess.mount('http://', adapter)
              sess.mount('https://', adapter)

              ## Pull weather data
              temp_df = temp_df.progress_apply(lambda x: self._pull_openWeatherMap_data(self, row = x, tf=tzf, session=sess, OWM_API_KEY=OWM_API_KEY),axis=1)

              ## Append new Weather data to weather data file
              temp_df.drop(['GAME_DATE','GAMETIME','YEAR','STADIUM','LATITUDE', 'LONGITUDE'],axis=1).to_csv(self.weather_file, mode='a',index=False,header=False)

       
       def pull_dk_sportsbook_odds(self):
              """
              SOURCED FROM: github.com/agad495/DKscraPy
              
              Scrapes current NFL game lines on Draftkings Sportsbook.

              """
              dk_api = requests.get("https://sportsbook.draftkings.com/sites/US-CO/api/v5/eventgroups/88808?format=json").json()
              dk_markets = dk_api['eventGroup']['offerCategories'][0]['offerSubcategoryDescriptors'][0]['offerSubcategory']['offers']

              games = {}
              for i in dk_markets:
                     if i[0]['outcomes'][0]['oddsDecimal'] == 0: # Skip this if there is no spread
                            continue
                     away_team = i[0]['outcomes'][0]['label']
                     home_team = i[0]['outcomes'][1]['label']
                     
                     if away_team not in games: 
                            # Gotta be a better way then a bunch of try excepts
                            games[away_team] = {'location':0}
                            try:
                                   games[away_team]['moneyline'] = i[2]['outcomes'][0]['oddsAmerican']
                            except:
                                   games[away_team]['moneyline'] = 'NULL_VALUE'
                            try:
                                   games[away_team]['spread'] = [i[0]['outcomes'][0]['line'], i[0]['outcomes'][0]['oddsAmerican']]
                            except:
                                   games[away_team]['spread'] = 'NULL_VALUE'
                            try:
                                   games[away_team]['over'] = [i[1]['outcomes'][0]['line'], i[1]['outcomes'][0]['oddsAmerican']]
                            except:
                                   games[away_team]['over'] = 'NULL_VALUE'
                            try:
                                   games[away_team]['under'] = [i[1]['outcomes'][1]['line'], i[1]['outcomes'][1]['oddsAmerican']]
                            except:
                                   games[away_team]['under'] = 'NULL_VALUE'
                            games[away_team]['opponent'] = home_team
                     
                     if home_team not in games:
                            games[home_team] = {'location':1}
                            try:
                                   games[home_team]['moneyline'] = i[2]['outcomes'][1]['oddsAmerican']
                            except:
                                   games[home_team]['moneyline'] = 'NULL_VALUE'
                            try:
                                   games[home_team]['spread'] = [i[0]['outcomes'][1]['line'], i[0]['outcomes'][1]['oddsAmerican']]
                            except:
                                   games[home_team]['spread'] = 'NULL_VALUE'
                            try:
                                   games[home_team]['over'] = [i[1]['outcomes'][0]['line'], i[1]['outcomes'][0]['oddsAmerican']]
                            except:
                                   games[home_team]['over'] = 'NULL_VALUE'
                            try:
                                   games[home_team]['under'] = [i[1]['outcomes'][1]['line'], i[1]['outcomes'][1]['oddsAmerican']]
                            except:
                                   games[home_team]['under'] = 'NULL_VALUE'     
                            games[home_team]['opponent'] = away_team

              games = pd.DataFrame(games).T.reset_index()

              with pd.ExcelWriter(self.ELO_visualization_file, mode='a', if_sheet_exists='overlay') as writer:
                     games.to_excel(      excel_writer= writer,
                                          sheet_name = 'Betting Outcomes',
                                          index=False,
                                          header=False, 
                                          startrow = 2,
                                          startcol = 35)
              
