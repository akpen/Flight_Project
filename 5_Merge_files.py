from pyspark.sql.functions import *
import csv
import datetime
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from geopy.distance import vincenty
from pyspark.sql import Row
import csv
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql import Row
import csv
from pyspark.sql import SQLContext
import shapely as shp
from shapely.geometry import Point, Polygon
import shapely as shp
import pandas as pd
import geopandas as gpd


def parseCSV(idx, part):
    if idx==0:
        part.next()
    for p in csv.reader(part):
        yield Row(Airport_IATA= p[0], Airport_Name = p[1], Airport_City = p[2], Country_name = p[3], Latitude = p[4], Longitude = p[5])


def parseCSV1(idx, part):
        if idx == 0:
            part.next()
        for p in csv.reader(part):
            yield Row(Airport= p[1], origin_count= p[2], dest_count= p[3], Total_Flights= p[4])

def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    rows = sc.textFile('../lmf445/Flight_Project/Data/Airport Codes8.csv').mapPartitionsWithIndex(parseCSV)
    airports = sqlContext.createDataFrame(rows)
    #airports = sqlContext.read.load('Flight_Project/Data/Airport Codes8.csv', format='csv', header=True, inferSchema=True)
    airports = airports.select('*').withColumnRenamed('Airport_IATA', 'Airport')
    airports = airports.select('*').withColumnRenamed('Airport_Name', 'Name')
    airports = airports.select('*').withColumnRenamed('Airport_City', 'City')
    airports = airports.drop('Country_name')
    rows2 = sc.textFile('../lmf445/MostBussyAirport_new.csv').mapPartitionsWithIndex(parseCSV1)
    busyairports = sqlContext.createDataFrame(rows2)
    #busyairports = sqlContext.read.load('Flight_Project/Data/MostBussyAirport.csv', format='csv', header=True, inferSchema=True)
    busyairports = busyairports.select('*', coalesce(busyairports["origin_count"], lit(0.0)).alias('clean_origin_count'))
    busyairports = busyairports.drop('origin_count')
    busyairports = busyairports.select('*', coalesce(busyairports["dest_count"], lit(0.0)).alias('clean_dest_count'))
    busyairports = busyairports.drop('dest_count')
    busyairports = busyairports.select('*', coalesce(busyairports["Total_Flights"], lit(0.0)).alias('clean_Total_Flights'))
    busyairports = busyairports.drop('Total_Flights')
    busyairports = busyairports.select('*', (busyairports.clean_origin_count + busyairports.clean_dest_count).alias(
        'Total_Flights'))
    busyairports = busyairports.filter(busyairports.Airport != 'DEST')
    busyairports = busyairports.filter(busyairports.Airport != 'ORIGIN')
    busyairports = busyairports.drop('clean_Total_Flights')
    #busyairports = busyairports.drop('_c0')
    busyairports = busyairports.select('*').withColumnRenamed('clean_dest_count', 'dest_count')
    busyairports = busyairports.select('*').withColumnRenamed('clean_origin_count', 'origin_count')
    locAirports = airports.join(busyairports, on="Airport", how='inner')
    locAirports.toPandas().to_csv('Output/Airports_Location_Counts.csv')

    pdlocAirports = locAirports.toPandas()
    geometry = []
    for air in pdlocAirports.Airport:
        geometry.append(shp.geometry.Point(pdlocAirports.loc[pdlocAirports.Airport == air, 'Longitude'],
                                           pdlocAirports.loc[pdlocAirports.Airport == air, 'Latitude']))
    crs = {'init': 'epsg:3857'}
    pdlocAirports = gpd.GeoDataFrame(pdlocAirports, crs=crs, geometry=geometry)
    with open('Data/Airport_Codes_Lat_Lon.geojson', 'w') as f:
        f.write(pdlocAirports.to_json())

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
