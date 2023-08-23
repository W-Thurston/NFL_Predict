# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class WriteItemPipeline(object):
	def __init__(self):
		self.filename = 'NFL_wk_by_wk.csv'
	def open_spider(self, spider):
		self.file = open(self.filename, 'w')
	def close_spider(self, spider):
		self.file.close()

	def process_item(self, item, spider):
		line = ','.join(item.values()) + '\n'
		self.file.write(line)
		return item
