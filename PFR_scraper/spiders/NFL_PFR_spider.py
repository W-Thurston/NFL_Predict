import scrapy
from scrapy import Spider
from scrapy.selector import Selector

import re

from PFR_scraper.item import NFL_ProFootballReference



class NFLSpider(Spider):
	name = 'TwitterBot'
	allowed_urls = ['https://www.pro-football-reference.com']
	start_urls = [f'https://www.pro-football-reference.com/years/{x}/games.htm' for x in range(1991,2023)]

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
			year 			 = str(response.url)[-12:-10]+"-"+str(int(str(response.url)[-12:-10])+1)

			boscore_url = 'https://www.pro-football-reference.com' + boxscore_link

			yield scrapy.Request(boscore_url, callback=self.parse_boxscores, meta={ "week_num_meta"         : week_num,
																					"game_day_of_week_meta" : game_day_of_week,
																					"game_date_meta"        : game_date,
																					"gametime_meta"         : gametime,
																					"winner_meta"           : winner,
																					"game_location_meta"    : game_location,
																					"loser_meta"            : loser,
																					"boxscore_link_meta"    : boxscore_link,
																					"pts_win_meta"          : pts_win,
																					"pts_lose_meta"         : pts_lose,
																					"yards_win_meta"        : yards_win,
																					"to_win_meta"           : to_win,
																					"yards_lose_meta"       : yards_lose,
																					"to_lose_meta"          : to_lose,
																					"year_meta"             : year})
	
	def parse_boxscores(self, response):

		item = NFL_ProFootballReference()
		item['week_num'] 		 = response.meta["week_num_meta"] 		  # Week number in season
		item['game_day_of_week'] = response.meta["game_day_of_week_meta"] # Day of week 
		item['game_date'] 		 = response.meta["game_date_meta"] 		  # Month & day
		item['gametime'] 		 = response.meta["gametime_meta"] 		  # Game Time, EST
		item['winner'] 			 = response.meta["winner_meta"] 	   	  # Winning team name 
		item['game_location'] 	 = response.meta["game_location_meta"] 	  # "@" if winning team was away
		item['loser'] 			 = response.meta["loser_meta"] 	  	  	  # Losing team name
		item['boxscore_link'] 	 = response.meta["boxscore_link_meta"] 	  # Link to boxscores
		item['pts_win'] 		 = response.meta["pts_win_meta"] 	   	  # Points scored by winning team
		item['pts_lose'] 		 = response.meta["pts_lose_meta"] 		  # Points scored by losing team 
		item['yards_win'] 		 = response.meta["yards_win_meta"] 	      # Yards gained by winning team
		item['to_win'] 			 = response.meta["to_win_meta"]   		  # Turnovers by the winning team
		item['yards_lose'] 		 = response.meta["yards_lose_meta"] 	  # Yards gained by losing team
		item['to_lose'] 		 = response.meta["to_lose_meta"]   		  # Turnovers by the losing team
		item['year']			 = response.meta["year_meta"]			  # Ex: '91-92'
		item['stadium']			 = response.xpath('//body/div[1]/div[@class="box"]/div[@class="scorebox"]/div[@class="scorebox_meta"]/div/a/text()').extract_first() if response.xpath('//body/div[1]/div[@class="box"]/div[@class="scorebox"]/div[@class="scorebox_meta"]/div/a/text()').extract_first() else "NULL_VALUE"

		## Game info data -- Pull data from the "Game Info" grid on the boxscore pages

		## regex to remove HTML comments strings
		regex = re.compile(r'<!--(.*)-->', re.DOTALL)

		## pull all comments from webpage and use above regex
		comments = response.xpath('//comment()').re(regex)

		## Pull out the one comment we'd like from the above response
		comment = [x for x in comments if 'div_game_info' in x][0]

		## Makes it so we can use .xpath() on the above html string
		commentsel = Selector(text=comment, type="html")

		## Pulls out all the row values for the "Game Info" grid on the boxscore pages (_info==left column, _stat==right column)
		game_data_info = [x.strip() for x in commentsel.xpath('//div[@id="div_game_info"]//th[@data-stat="info"]//text()').extract()]
		game_data_stat = [x.strip() for x in commentsel.xpath('//div[@id="div_game_info"]//td[@data-stat="stat"]//text()').extract() if x not in ['(over)','(under)']]
		
		## Initialize thes values
		item['roof']        = None
		item['surface']     = None
		item['vegas_line']  = None
		item['over_under']  = None
		item['temperature'] = None
		item['humidity']    = None
		item['wind']        = None

		## Loop over "titles" for each row in the game_info grid and assign the appropriate value from the right column
		for idx, val in enumerate(game_data_info):
			val = val.lower()
			if val == 'roof':
				item['roof'] = game_data_stat[idx]
			elif val == 'surface':
				item['surface'] = game_data_stat[idx]
			elif val == 'vegas line':
				item['vegas_line'] = game_data_stat[idx]
			elif val == 'over/under':
				item['over_under'] = game_data_stat[idx]
			elif val == 'weather*' or val == 'weather':
				weather = game_data_stat[idx].split(',')
				if len(weather) == 3:
					item['temperature'] = weather[0].strip()
					item['humidity']    = weather[1].strip()
					item['wind']        = weather[2].strip()
				elif len(weather) == 2:
					item['temperature'] = weather[0].strip()
					item['wind']        = weather[1].strip()
					item['humidity']    = 'NULL_VALUE'
			elif val == 'attendance':
				continue
			elif val == 'won toss':
				continue
			elif val == 'won ot toss':
				continue
			elif val == 'super bowl mvp':
				continue
			elif val == 'duration':
				continue
			else:
				print(f"This was an un-caught info in all_game_stats: {val}")

		## If any of the values we want did not appear on the page, set it to "NULL_VALUE"
		for val in ['roof','surface','vegas_line','over_under','temperature','humidity','wind']:
			if item[val] is None:
				item[val] = 'NULL_VALUE'

		yield item
