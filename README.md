# Analysis for domestic flights in the United States


### By Arnav Somani, Fernando Melchor, Hrafnkell Hjorleifsson, and Subhansu Gupta.

##### Abstract—With the increasing rate at which data is being generated, it is necessary to develop skills and tools to better manage these large quantities of data. The value of these big data-sets will depend on the ability to perform on-time analysis, inference and the scalability of the developed pipeline. In this project, we use big data tools to analysis the USA domestic flight system and get facts and trends of interest to general public and stakeholders.



<img src="/Images/test.png" width="808">








#### Introduction

#### Data & Methodologies


#### A. Data
The data used was threefold; aviation data, geospatial information for airports and geo-spatial information on states, more details below:
*	The aviation data comes from the United States Department of Transportations Bureau of Transportation Statistics. The data is filtered by month with the option of downloading data for individual states or the whole United States at once. For this project data was collected for the whole year of 2014 through 2016 for the whole country.Total number of rows/flights are around 17 million.
*	The geospatial information for the different airports, it contains a list of all identified airports in the USA and their Airport Codes, information about the longitude and latitude is included in the file and as well information about the city where the airport is located.
*	The geospatial information for the different states comes from the USA Census Bureau, it contains the geographical political boundaries of all the USA territories. 


#### B. Big Data Challenges
Filtering and aggregating 17 million rows of data in various ways is computationally heavy and time consuming. By filtering and aggregating the data with spark scripts running on a cluster environment we minimize it by a magnitude of 10^4 - 10^6 depending on the granularity of the outputted csv files.
Pivot tables, that are computationally heavy to process, were created in order to analyze the daily flights like time-series grouping them by airline, airport or city. This output csv files are very useful for different kinds of analysis and they can be processed without the need of a big computational power. 
We designed the outputs of each script to be efficient and easy to handle for analysis proposes. We enrich our outputs by merging them with more specific information about the airports, like geo-location, names and cities.


#### C. Network Analysis 

We used the total routes to get all the airports that were connected through flights, in this way a network with 334 airports/nodes was created to represent the whole system. To better analyze this system, the network was reduced by grouping the airports by states, this was done by using the states shapefiles and performing the geometric operation of point with in a polygon to determine if certain airport was within a state. This was necessary because the airport data that we had did not have a feature identifying the state of the airport. After reducing the network by state the network had 55 airports/nodes 


<img src="/Images/test2.png" width="808">


With this reduced network, we measured the average shortest path length from one origin to all the possible destinations. Each state had a measure of connectivity that could be interpreted as the average number of connecting flights needed to go from a particular state to all the others. Most connected states are Illinois, Georgia, Texas, Colorado and Minnesota. The least connected states/territories are Northern Mariana Islands, American Samoa, Guam, Mississippi and Delaware.


<img src="/Images/connectivity_score2.png" width="808">

#### D. Time Series

The busiest (no. landings and takeoffs) airport for domestic flights in America, by some margin, is Hartsfield-Jackson Atlanta International Airport in Atlanta Georgia.

<img src="/Images/8_busiest_airports_monthly.png" width="808">
>
<img src="/Images/8_busiest_airports_monthly_normalized.png" width="808">
>
Somewhat obviously the busiest city is Atlanta, Georgia, though Chicago, Illinois, recorded more flights for the period June 2014 through December 2015.
Interestingly a drop in flights through Chicago can be seen in January 2016. The reason for this is not clear though a quick internet search seems to suggest that at
least a part of the reason might be due to employment of larger aircraft vessels. New York is the 7th busiest city in America but if Newark airport is included it rises to
3rd.

<img src="/Images/8_busiest_cities_monthly.png" width="808">
>
<img src="/Images/8_busiest_cities_monthly_normalized.png" width="808">

The busiest route by some margin is Los Angeles - San Francisco, followed by Los Angeles - New York and Los Angeles - Las Vegas, figure 4. These are calculated
irrespective of flight direction but there is a close to 1:1 matching between all legs. 


<img src="/Images/8_busiest_routes_monthly_averaged.png" width="808">


The airport that has seen the most increase in flights is SeattleTacoma International Airport and the airport that has seen the most decrease is Dallas/Fort Worth International Airport, figures 5 and 6. The same results are found when looking at the data on city level.

<img src="/Images/increase_airports.png" width="808">

<img src="/Images/decrease_airports.png" width="808">

The route that saw the most increase in traffic was Dallas - LaGuardia and the route that saw the most decline in traffic was Los Angeles - San Diego.

<img src="/Images/8_busiest_routes_monthly.png" width="808">

<img src="/Images/decrease_routes.png" width="808">

