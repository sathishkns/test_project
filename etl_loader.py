# etl_loader.py

import json
from etl_build import utils,logger,start_spark

class ETLloader:
    def __init__(self):
        self.spark,self.log,self.config = start_spark(app_name='test_app')
        
        self.config_dict = self.get_yaml_config()


    def get_yaml_config(self,load_type,dest_table):
        """
        parse the yaml file to get the different load type layers
        
        """
        config_prefix = f"/etl_build/config/{yaml_file_name}"
        if self.load_type=='bronze':
            config_file_path=f"{config_prefix}/bronze_config_{self.dest_table}.yaml"
        elif self.load_type=='silver':
            config_file_path=f"{config_prefix}/silver_config_{self.dest_table}.yaml"
        elif self.load_type=='gold':
            config_file_path=f"{config_prefix}/gold_config_{self.dest_table}.yaml"
        else:
            raise InvalidLoadType("unknown load type")
            
        config_path = utils.read_yaml(config_file_path)
        return config_path
        
    def append_delta_table(df,tgt_db,tgt_tbl_name):
        try:
            df.write.format('delta').mode('append').saveAsTable("{0}{1}".format(tgt_db,tgt_tbl_name))
        except Exception as e:
            raise "append to delta table failed"
    
    
    def upsert_delta_table(self,src_df,dest_db,dest_table):
        """
        write logic to perform insert or upsert based on the key column between source and target dataframe
        
        """
            
    def extract(self,extracts=None):
        if extracts:
            read_data = f"/etl_build/config/{yaml_file_name}"
            for extract in extracts:
                if extract['exec']=='read_table':
                    """
                    read catalog table and pass to source dataframe
                    
                    """
                    
        return df_src
        
    def transform(self, transforms=None):
        if transforms:
            for transform in transforms:
                if transform['exec']=='process_sql':
                    sql_file = f"/path/to/sql"
                    transformed_df = self.spark.sql(sql_file)
                    
            return transformed_df
            
    def load(self,loads=None):
        try:
        if loads:
            for load_yaml in loads:
                if load_yaml['exec']=='upsert_delta_table':
                    upsert_delta_table()
                    append_delta_table(final_df,dest_table,dest_db)
        except Exception as err:
            msg = f"upsert to delta table failed"
            self.logger(msg)
            raise err
