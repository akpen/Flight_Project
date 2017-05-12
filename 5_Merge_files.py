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
        yield Row(ORIGIN=p[14],
                  ORIGIN_AIRPORT_ID = str(p[11]),
                  DEST = p[23],
                  DEST_AIRPORT_ID = str(p[20]),
                  ROUTE = (p[11],p[20]))
def main(sc):
    spark = HiveContext(sc)
    sqlContext = HiveContext(sc)
    #airports = sqlContext.read.load('Flight_Project/Data/Airport Codes8.csv', format='csv', header=True, inferSchema=True)
    #airports = airports.select('*').withColumnRenamed('Airport (IATA)', 'Airport')
    #airports = airports.select('*').withColumnRenamed('Airport (Name)', 'Name')
    #airports = airports.select('*').withColumnRenamed('Airport (City)', 'City')
    #airports = airports.drop('Country (Name)')
    busyairports = sqlContext.read.load('Flight_Project/Data/MostBussyAirport.csv', format='csv', header=True, inferSchema=True)
    busyairports = busyairports.select('*',
                                       coalesce(busyairports["origin_count"], lit(0.0)).alias('clean_origin_count'))
    busyairports = busyairports.drop('origin_count')
    busyairports = busyairports.select('*', coalesce(busyairports["dest_count"], lit(0.0)).alias('clean_dest_count'))
    busyairports = busyairports.drop('dest_count')
    busyairports = busyairports.select('*',
                                       coalesce(busyairports["Total_Flights"], lit(0.0)).alias('clean_Total_Flights'))
    busyairports = busyairports.drop('Total_Flights')
    busyairports = busyairports.select('*', (busyairports.clean_origin_count + busyairports.clean_dest_count).alias(
        'Total_Flights'))
    busyairports = busyairports.filter(busyairports.Airport != 'DEST')
    busyairports = busyairports.filter(busyairports.Airport != 'ORIGIN')
    busyairports = busyairports.drop('clean_Total_Flights')
    busyairports = busyairports.drop('_c0')
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
