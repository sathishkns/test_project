## logger.py

class Log4j:
	def __init__(self,spark):
		conf = spark.sparkContext.getConf()
		app_id = conf.get("app_id")
		app_name=conf.get("app_name")
		log4j = spark.jvm.org.apache.log4j
		
		self.logger = log4j.getLogger("log Message")
		
	def error(self, message):
		self.logger.error(message)
		
	def warn(self, message):
		self.logger.warn(message)
		
	def info(self, message):
		self.logger.info(message)
		
	def debug(self, message):
		self.logger.debug(message)


# utils.py

import json
impot yaml

def parse_json(conf_file):
	with open(conf_file) asa conf:
		conf_json = json.load(conf)
	return conf_json
	
def read_json(file_path) ->json:
	with open(file_path,'r') as json_data:
		return json.load(json_data)
		
def write_json(file_path,json_object):
	with open(file_path,'w') as json_file:
		json_file.write(json_object)
		
def convert_yaml_to_dict(file_path) -> dict:
	with open(file_path,'r) as yaml_data:
		return yaml.safe_load(yaml_data)
		
		
### start_spark.py

import json
from pyspark.sql import SparkSession
from etl_build import Log4j


def start_spark(app_name, config_file):
	spark.SparkSession.builder.appName(app_name).getOrCreate()
	spark_logger = Log4j(spark)
	
	if config_file:
		with open(config_file,'r') as read_file:
			config_dict = json.load(read_file)
		spark_logger.warn(f"loaded config file - {config_file}")
    else:
        spark_logger.warn("no config file")
        config_dict=None
        
    return spark, spark_logger,config_dict
