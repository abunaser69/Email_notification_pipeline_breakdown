def get_extract():
  
  table1_sql =  "select * from table1 \
                             where system_created_date  >= (select current_date) \
                             and (row_count_loaded is null or row_count_loaded  !=0) \
                             order by system_created_date desc"

  table2_sql =  "select * from table2 \
                                   where system_created_date  >= (select current_date) \
                                   order by system_created_date desc"

  table3_sql =  "select * from table3 \
                                           where system_created_date  >= (select current_date) \
                                           order by submission_id desc"

  df1_pandas = spark.sql(table1_sql).toPandas()
  df2_pandas = spark.sql(table2_sql).toPandas()
  df3_pandas = spark.sql(table3_sql).toPandas()
  df4_pandas = df1_pandas.append([df2_pandas, df3_pandas], ignore_index = False).fillna('')
  
  return  df4_pandas

date =  dt.datetime.strftime (dt.datetime.today(), "%Y-%m-%d")
filename = f"{extract}_{date}" 
print(filename)
    
# create extract
df_extract_pandas = get_extract()
#convert pandas df to pyspark dataframe
df_extract = spark.createDataFrame(df_extract_pandas.astype(str))
