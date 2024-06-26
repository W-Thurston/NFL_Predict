# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class HistoricalPFRWriteItemPipeline(object):
	def __init__(self):
		self.filename = 'data/raw/NFL_wk_by_wk.csv'
	def open_spider(self, spider):
		self.file = open(self.filename, 'w')
	def close_spider(self, spider):
		self.file.close()

	def process_item(self, item, spider):
		line = ','.join(item.values()) + '\n'
		self.file.write(line)
		return item
	
class AppendNewPFRWriteItemPipeline(object):
	def __init__(self):
		self.filename = 'data/raw/NFL_wk_by_wk.csv'
	def open_spider(self, spider):
		self.file = open(self.filename, 'a')
	def close_spider(self, spider):
		self.file.close()

	def process_item(self, item, spider):
		line = ','.join(item.values()) + '\n'
		self.file.write(line)
		return item

class UpcomingScheduleWriteItemPipeline(object):
	def __init__(self):
		self.filename = 'data/raw/NFL_upcoming_schedule.csv'
	def open_spider(self, spider):
		self.file = open(self.filename, 'w')
	def close_spider(self, spider):
		self.file.close()

	def process_item(self, item, spider):
		line = ','.join(item.values()) + '\n'
		self.file.write(line)
		return item
