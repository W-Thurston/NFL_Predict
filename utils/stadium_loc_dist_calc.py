import re
from geopy.distance import distance
import datetime
import timezonefinder, pytz


def _convert(tude):
    '''
    
    '''
    
    ## If the latiTUDE or longiTUDE are in the form [Degrees°Minutes′Secounds″Direction]
    if '″' in tude:

        ## Multiplier is 1 for North/East & -1 for South/West
        multiplier = 1 if tude[-1] in ['N', 'E'] else -1

        ## Split data with Degree/Minute/Seconds symbols
        deg, minutes, seconds, direction = re.split('[°\′″]', tude)

        ## convert above split data into a +/- decimal representation of latiTUDE or longiTUDE
        decimal_degrees = round(multiplier * (float(deg) + float(minutes)/60 + float(seconds)/(60*60)),4)

        #print(f"{tude} converted to {decimal_degrees}")
        return decimal_degrees
    
    ## If the latiTUDE or longiTUDE are in the form [Degrees°]
    else:
        
        ## Split data with Degree symbols
        decimal_degrees, direction = re.split('[°]', tude)

        ## Multiplier is 1 for North/East & -1 for South/West
        multiplier = 1 if direction in ['N', 'E'] else -1

        ## convert above split data into a +/- decimal representation of latiTUDE or longiTUDE
        decimal_degrees = round(multiplier * float(decimal_degrees),4)

        #print(f"{tude} converted to {decimal_degrees}")
        return decimal_degrees

def _measure_distance(lat_long_of_home:list, lat_long_of_game:list, metric:str='km') -> float:
    '''
    
    '''

    ## Convert string value of either Degrees, Minutes, Seconds, Direction or Degrees, Direction to +/- Degrees
    lat_of_home_to_decimal  = _convert(lat_long_of_home[0])
    long_of_home_to_decimal = _convert(lat_long_of_home[1])
    lat_of_game_to_decimal  = _convert(lat_long_of_game[0])
    long_of_game_to_decimal = _convert(lat_long_of_game[1])
    
    ## Place above converted values into tuple
    coords_of_home = (lat_of_home_to_decimal, long_of_home_to_decimal)
    coords_of_game = (lat_of_game_to_decimal, long_of_game_to_decimal)

    ## If you want km or miles; then round the wanted distance to nearest 2 decimal places
    if metric=='km':
        #print(f"Distance between:\n\t{coords_of_home}\n\t{coords_of_game}\n\t{round(distance(coords_of_home, coords_of_game).km,2)} km")
        return round(distance(coords_of_home, coords_of_game).km,6)
        
    elif metric=='miles':
        #print(f"Distance between:\n\t{coords_of_home}\n\t{coords_of_game}\n\t{round(distance(coords_of_home, coords_of_game).miles,2)} miles")
        return round(distance(coords_of_home, coords_of_game).miles,6)
    

def _find_utc_offset(latitude, longitude, tf):
    '''
    
    '''

    ## From the lat/long, get the tz-database-style time zone name (e.g. 'America/New York') or None
    timezone_str = tf.certain_timezone_at(lat=_convert(latitude), lng=_convert(longitude))

    ## From timezone name get UTC offset (e.g. '-0500' for America/New York)
    timezone = pytz.timezone(timezone_str).localize(datetime.datetime.now()).strftime('%z')
    # timezone = pytz.timezone(timezone_str).localize(datetime.datetime(2020,12,1)).strftime('%z')  ## Leaving this here so that I can implement based on specific date

    return timezone


def _calculate_timezone_difference(lat_x, long_x, lat_y, long_y, tz_find):
    '''
    
    '''

    timezone_x = int(_find_utc_offset(lat_x, long_x, tf=tz_find))
    timezone_y = int(_find_utc_offset(lat_y, long_y, tf=tz_find))

    timezone_diff = timezone_x - timezone_y

    return timezone_diff