import re
from geopy.distance import distance


def convert(tude):

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

def measure_distance(lat_of_home, long_of_home, lat_of_game, long_of_game, metric='km'):
    
    ## Convert string value of either Degrees, Minutes, Seconds, Direction or Degrees, Direction to +/- Degrees
    lat_of_home_to_decimal  = convert(lat_of_home)
    long_of_home_to_decimal = convert(long_of_home)
    lat_of_game_to_decimal  = convert(lat_of_game)
    long_of_game_to_decimal = convert(long_of_game)
    
    ## Place above converted values into tuple
    coords_of_home = (lat_of_home_to_decimal, long_of_home_to_decimal)
    coords_of_game = (lat_of_game_to_decimal, long_of_game_to_decimal)

    ## If you want km or miles; then round the wanted distance to nearest 2 decimal places
    if metric=='km':
        print(f"Distance between:\n\t{coords_of_home}\n\t{coords_of_game}\n\t{round(distance(coords_of_home, coords_of_game).km,2)} km")
        return round(distance(coords_of_home, coords_of_game).km,2)
        
    elif metric=='miles':
        print(f"Distance between:\n\t{coords_of_home}\n\t{coords_of_game}\n\t{round(distance(coords_of_home, coords_of_game).miles,2)} miles")
        return round(distance(coords_of_home, coords_of_game).miles,2)