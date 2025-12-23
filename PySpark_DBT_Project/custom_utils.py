from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

class transformationss:
    def dedup(self,df:DataFrame,dedup_cols:list,cdc:str):

        # dedup_list
        df = df.withColumn("dedupKey",concat(*dedup_cols))
        df = df.withColumn("dedupCounts",row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col('dedupCounts')==1)
        df = df.drop('dedupKey','dedupCounts')
        return df



