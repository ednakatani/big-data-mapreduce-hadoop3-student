## Big Data Processing with MapReduce

You and your team were hired to process data using MapReduce. Your company has access to a 
dataset with commercial transactions between countries during the past 30 years. For each transaction, 
the dataset contains the following variables:

|ID|Variable (column)| Description|
|--|-----------------|------------|
|00|Country |Country involved in the commercial transaction|
|01|Year |Year in which the transaction took place|
|02|Commodity code |Commodity identifier|
|03|Commodity |Commodity description|
|04|Flow |Flow, e.g. Export or Import|
|05|Price |Price, in USD|
|06|Weight |Commodity weight|
|07|Unit |Unit in which the commodity is measured, e.g. Number of items|
|08|Amount |Commodity amount given in the aforementioned unit|
|09|Category |Commodity category, e.g. Live animals|


Given the aforementioned context, you are in charge of developing a set of solutions that allow 
the company to answer the following demands:
1. (Easy) The number of transactions involving Brazil;
2. (Easy) The number of transactions per year;
3. (Easy) The number of transactions per flow type and year;
4. (Easy) The average of commodity values per year;
5. (Easy) The average price of commodities per unit type, year, and category in the export flow 
in Brazil;
6. (Medium) The maximum, minimum, and mean transaction price per unit type and year;
7. (Hard) The most commercialized commodity (summing the quantities) in 2016, per flow 
type.
