# Data Munging/Data Munger


## Operation of a sql query in backend


**This is operational workout of what happens when a sql query is provided**

>Usage of only Core Java concepts throughout



**Data munger and Data Munging are the same whereas the variation is Data Munging is a processor based project**

>Need to enhance by reducing the amount of for loops and if conditions



## Operational OverView
>Parsing the provided sql string query
	1. Detecting what type of query is it?
	2. Taking the filelocation from the query.
	3. Detecting the presence of specific columns
	4. Detecting the where conditions and split and store them
	5. Looking for the logical operators between two conditions in case of multiple where conditions

>Retrieving the Header row i.e., the first row from the csv doc which contains the name of the fields

>Working with query with single where query and store it in a map and display it

>Working with multiple conditions first checking the conditions and only if it passess the respective row data is stored in a map/data structure

>Ordering of data in case of presence of order by condition

>Similarly the above for group by conditions

>Last but not the least the aggregate functions, this might come in any order


### The above has been implemented in Data Munger the variation in Data Munging is.............

>Created a common interface, which is inherited by multiple classes(3)

>One class for Simple query Type(with and without where conditions)

>One class for Group by and order by type

>And last for Aggregate type



# Happy Programming

