from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import argparse


def transform_data(spark,data_source : str):
    """
    
    
    """
    
    menu = spark.read.csv(f'{data_source}restaurant-menus.csv', header=True, inferSchema=True)
    restau = spark.read.csv(f'{data_source}restaurants.csv', header=True, inferSchema=True)

    # remove usd in the price 
    menu = menu.withColumn('price', F.split(menu['price'], " ")[0])

    # cast the column type 
    menu = menu.withColumn('price', menu['price'].cast(FloatType()))

    # remove nulls
    menu = menu.filter((F.col('price').isNotNull()) & (F.col('restaurant_id').isNotNull())
                                & (F.col('category').isNotNull()))
    
    # fill with 0 null records in score and ratings 
    restau = restau.withColumn('score', restau['score'].cast(FloatType())) \
                    .withColumn('ratings', restau['ratings'].cast(FloatType())) 

    restau = restau.fillna(0, subset=['score', 'ratings'])

    #normalize the price range column
    restau = restau.withColumn('price_range',
                                F.when((F.col('price_range')=='$'), "Inexpensive") \
                                .when(F.col('price_range')=='$$', "Moderately expensive") \
                                .when(F.col('price_range') == "$$$", "Expensive") \
                                .when(F.col('price_range')=='$$$$', "Very Expensive") \
                                .otherwise("No price range"))
    
    #create the city and state 
    restau = restau.withColumn('city', F.split(restau["full_address"], ', ')[1]) \
                        .withColumn('state', F.split(restau["full_address"], ', ')[2])
        
    return restau, menu

 
def create_table(spark, restau, menu, output:str):
    """
    
    
    """
    # Restaurant table
    restau_table = restau.dropDuplicates(subset=['name']) \
                            .withColumn("restaurant_id", F.monotonically_increasing_id()) \
                            .select(["restaurant_id", "category","name", "price_range"]) \
                            .withColumnRenamed('category', 'restaurant_category')\
                            .withColumnRenamed('name', 'restaurant_name') \
                            .withColumnRenamed('price_range', 'restaurant_price_range') \
                            .orderBy(F.col('restaurant_id').asc())

    # Address_table 
    address_table = restau.dropDuplicates(["full_address"]) \
                            .join(restau_table, (restau['name']==restau_table['restaurant_name']) & (restau['category']==restau_table['restaurant_category']) & 
                              (restau['price_range']==restau_table['restaurant_price_range']), how="inner") \
                            .withColumn("address_id", F.monotonically_increasing_id()) \
                            .select(["address_id","restaurant_id","city", "state","zip_code","lat", "lng", "full_address"]) \
                            .orderBy(F.col("address_id").asc())
    
    # Menu table
    menu_table = menu.dropDuplicates()\
                        .withColumn("menu_id", F.monotonically_increasing_id()) \
                        .withColumnRenamed("name", "menu_name") \
                        .withColumnRenamed("category", "menu_category") \
                        .join(restau, (menu['restaurant_id']==restau['id']), how="left") \
                        .drop("restaurant_id") \
                        .join(restau_table, (restau['name']==restau_table['restaurant_name']) & (restau['category']==restau_table['restaurant_category']) & 
                              (restau['price_range']==restau_table['restaurant_price_range']), how="inner") \
                        .select(['menu_id','restaurant_id','menu_name', 'menu_category', 'description']) \
                        .orderBy(F.col('menu_id').asc())
    
    ### Load avro files 
    restau_table.write.format("avro").mode("overwrite").save(f'{output}/restaurant_table.avro')
    address_table.write.format("avro").mode("overwrite").save(f'{output}/address_table.avro')
    menu_table.write.format('avro').mode("overwrite").save(f'{output}/menu_table.avro')



if __name__ == '__main__': 
    with SparkSession.builder.appName('uber_eats_spark').getOrCreate() as spark: 
        parser = argparse.ArgumentParser()
        parser.add_argument('--data_source')
        parser.add_argument('--output')
        args = parser.parse_args()

        restau, menu = transform_data(spark, args.data_source)
        create_table(spark, restau, menu, args.output)