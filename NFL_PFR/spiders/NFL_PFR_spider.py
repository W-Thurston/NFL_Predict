from scrapy import Spider
from NFL_PFR.item import NFL_ProFootballReference
import pandas as pd

class NFLSpider(Spider):
	name = 'TwitterBot'
	allowed_urls = ['https://www.pro-football-reference.com']
	start_urls = [f'https://www.pro-football-reference.com/years/{x}/games.htm' for x in range(1991,2023)]

	# custom_settings = {
    #     'FEEDS': { 'NFL_wk_by_wk.csv': { 'format': 'csv','overwrite': True}}
    #     }

	def parse(self, response):

		rows = response.xpath('//tbody/tr')
		for row in rows:
			week_num 		 = row.xpath('./th[@data-stat="week_num"]/text()').extract_first()         if row.xpath('./th[@data-stat="week_num"]/text()').extract_first() else "NULL_VALUE"
			game_day_of_week = row.xpath('./td[@data-stat="game_day_of_week"]/text()').extract_first() if row.xpath('./td[@data-stat="game_day_of_week"]/text()').extract_first() else "NULL_VALUE"
			game_date 		 = row.xpath('./td[@data-stat="game_date"]/text()').extract_first()        if row.xpath('./td[@data-stat="game_date"]/text()').extract_first() else "NULL_VALUE"
			gametime 		 = row.xpath('./td[@data-stat="gametime"]/text()').extract_first()         if row.xpath('./td[@data-stat="gametime"]/text()').extract_first() else "NULL_VALUE"
			winner 	  		 = row.xpath('./td[@data-stat="winner"]/strong/a/text()').extract_first()  if row.xpath('./td[@data-stat="winner"]/strong/a/text()').extract_first() else row.xpath('./td[@data-stat="winner"]/a/text()').extract_first() if row.xpath('./td[@data-stat="winner"]/a/text()').extract_first() else "NULL_VALUE"
			game_location 	 = row.xpath('./td[@data-stat="game_location"]/text()').extract_first()    if row.xpath('./td[@data-stat="game_location"]/text()').extract_first() else "NULL_VALUE"
			loser 	  	  	 = row.xpath('./td[@data-stat="loser"]/a/text()').extract_first()          if row.xpath('./td[@data-stat="loser"]/a/text()').extract_first() else "NULL_VALUE"
			boxscore_link 	 = row.xpath('./td[@data-stat="boxscore_word"]/a/@href').extract_first()   if row.xpath('./td[@data-stat="boxscore_word"]/a/@href').extract_first() else "NULL_VALUE"
			pts_win 	  	 = row.xpath('./td[@data-stat="pts_win"]/strong/text()').extract_first()   if row.xpath('./td[@data-stat="pts_win"]/strong/text()').extract_first() else "NULL_VALUE"
			pts_lose 		 = row.xpath('./td[@data-stat="pts_lose"]/text()').extract_first()         if row.xpath('./td[@data-stat="pts_lose"]/text()').extract_first() else "NULL_VALUE"
			yards_win 	 	 = row.xpath('./td[@data-stat="yards_win"]/text()').extract_first()        if row.xpath('./td[@data-stat="yards_win"]/text()').extract_first() else "NULL_VALUE"
			to_win 			 = row.xpath('./td[@data-stat="to_win"]/text()').extract_first()           if row.xpath('./td[@data-stat="to_win"]/text()').extract_first() else "NULL_VALUE"
			yards_lose 		 = row.xpath('./td[@data-stat="yards_lose"]/text()').extract_first()       if row.xpath('./td[@data-stat="yards_lose"]/text()').extract_first() else "NULL_VALUE"
			to_lose  		 = row.xpath('./td[@data-stat="to_lose"]/text()').extract_first()          if row.xpath('./td[@data-stat="to_lose"]/text()').extract_first() else "NULL_VALUE"

			item = NFL_ProFootballReference()
			item['week_num'] 		 = week_num 		# Week number in season
			item['game_day_of_week'] = game_day_of_week # Day of week 
			item['game_date'] 		 = game_date 		# Month & day
			item['gametime'] 		 = gametime 		# Game Time, EST
			item['winner'] 			 = winner 	   		# Winning team name 
			item['game_location'] 	 = game_location 	# "@" if winning team was away
			item['loser'] 			 = loser 	  	  	# Losing team name
			item['boxscore_link'] 	 = boxscore_link 	# Link to boxscores
			item['pts_win'] 		 = pts_win 	   		# Points scored by winning team
			item['pts_lose'] 		 = pts_lose 		# Points scored by losing team 
			item['yards_win'] 		 = yards_win 	    # Yards gained by winning team
			item['to_win'] 			 = to_win   		# Turnovers by the winning team
			item['yards_lose'] 		 = yards_lose 		# Yards gained by losing team
			item['to_lose'] 		 = to_lose   		# Turnovers by the losing team
			item['year']			 = str(response.url)[-12:-10]+"-"+str(int(str(response.url)[-12:-10])+1)

			yield item


