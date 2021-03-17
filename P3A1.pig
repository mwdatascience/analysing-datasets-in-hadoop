/* Loading London Fire Dataset & Defining Data Schema  */

london_fire_data = LOAD '/home/training/londonfiredata.csv' using PigStorage(',') AS 
		(borough_code_lfd:chararray,
		borough_name_lfd:chararray,
		cal_year:chararray,
		date_lfd:chararray,
		Hour:chararray,
		incident:chararray);

/* Loading London Borough Dataset & Defining Data Schema  */

london_borough_data = LOAD '/home/training/boroughVariables.csv' using PigStorage(',') AS 
		(borough_code_lbd:chararray,
		borough_name_lbd:chararray,
		date_lbd:chararray,
		median_salary:chararray,
		life_satisfaction:chararray,
		mean_salary:chararray,
		recyling_pct:chararray,
		population_size:int,
		number_of_jobs:chararray,
		area_size:int,
		no_of_houses:int);

/* Joining the Two Datasets Together via use of an Inner JOIN */

london_borough_fire_join = JOIN london_fire_data BY borough_code_lfd, london_borough_data BY borough_code_lbd;

/* Group the Joined dataset together by Borough Name, Borough Population Size, Borough Area Size & Number of Houses Per Borough */

grouped = GROUP london_borough_fire_join BY (borough_name_lfd, population_size, area_size, no_of_houses);

/* For each instance in the group above, calculate the population density, the housing density and count the number of callouts per Borough  */

outputStats = FOREACH grouped GENERATE FLATTEN(group.borough_name_lfd),(group.population_size)/(group.area_size) AS popDensity, (group.no_of_houses)/(group.area_size) AS housingDensity, COUNT(london_borough_fire_join.borough_name_lfd) AS totalCallouts;

/* Output the Stats to the Terminal */
DUMP outputStats;

