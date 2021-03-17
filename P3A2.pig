/* Loading London Fire Dataset & Defining Data Schema  */

london_fire_data = LOAD '/home/training/londonfiredata.csv' using PigStorage(',') AS 
		(borough_code_lfd:chararray,
		borough_name_lfd:chararray,
		cal_year:chararray,
		date_lfd:chararray,
		Hour:chararray,
		incident:chararray);

/* Grouping data by callout hour  */

group_by_hour = GROUP london_fire_data BY (Hour);

/* For each callout hour perform the following */

callout_data = FOREACH group_by_hour {

	/* Filtering to Only Keep all Instances of when Borough Name is Westminster */

	west = FILTER london_fire_data by borough_name_lfd=='WESTMINSTER';

	/* The Total Count of Borough Callouts From the Filtered Westminster Data*/

	west_value = COUNT(west.borough_name_lfd);

	/* Filtering to Only Keep all Instances of When Borough Name is Camden*/

	camd = FILTER london_fire_data by borough_name_lfd=='CAMDEN';

	/* The Total Count of Borough Callouts from the Filtered Camden Data*/

	camd_value = COUNT(camd.borough_name_lfd);

	/* The Total Count of All Borough Callouts */

	total_value = COUNT(london_fire_data.borough_name_lfd);

	/* Generating the results */

	GENERATE group, total_value, west_value, camd_value;
	}

/* Dumping Results of FOREACH to the Terminal  */
DUMP callout_data;
