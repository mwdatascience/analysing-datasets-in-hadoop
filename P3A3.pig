/* Loading London Fire Dataset & Defining Data Schema  */

london_fire_data = LOAD '/home/training/londonfiredata.csv' using PigStorage(',') AS 
		(borough_code_lfd:chararray,
		borough_name_lfd:chararray,
		cal_year:chararray,
		date_lfd:chararray,
		Hour:chararray,
		incident:chararray);

/* Grouping data by Borough Name */

grouped = GROUP london_fire_data BY (borough_name_lfd);

/* Conducting the following for each member of the group above*/

incident_type = FOREACH grouped {

	/* Filtering to Only Keep all Instances of when Incident is False Alarm */

	false_alarms = FILTER london_fire_data by incident=='False Alarm';

	/* Counting the Total Value of False Alarms from the Filtered Data */

	false_alarm_value = COUNT(false_alarms.incident);	

	/* Filtering to Only Keep all Instances of when Incident is Special Service */

	special_data = FILTER london_fire_data by incident=='Special Service';

	/* Counting the Total Value of Special Services from the Filtered Data */

	special_data_value = COUNT(special_data.incident);

	/* Filtering to Only Keep all Instances of when Incident is Fire */

	fire_data = FILTER london_fire_data by incident=='Fire';

	/* Counting the Total Value of Fire from the Filtered Data */

	fire_data_value = COUNT(fire_data.incident);

	/* Counting the Total Value of All Incidents from the Main Dataset */

	total_value = COUNT(london_fire_data.incident);

	/* Generating the Results by Finding Percentages of Each Incident Type and Rounding to Nearest Integer */

	GENERATE group, ROUND(((double)false_alarm_value/(double)total_value)*100) AS falseAlarmPercentage, ROUND(((double)special_data_value/(double)total_value)*100) AS specialServicePercentage, ROUND(((double)fire_data_value/(double)total_value)*100) AS fireDataPercentage;
	}

/* Sorting the data by the False Alarm Percentage Descending */

sortedFalse = ORDER incident_type BY falseAlarmPercentage DESC;

/* Dumping the Sorted Data to the Terminal */

DUMP sortedFalse;


