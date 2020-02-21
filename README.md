A simple and, most importantly, lightweight library of functions for MySQL to be used with PDO
Only has support for application-level queries ie. SELECT, UPDATE, INSERT and DELETE.
Include the file, call new sqlParser() and either include sql statement in the constructor or call processSqlQuery('sql query here')
It will return an array including:
	 - a new statment with "?" instead of values
	 - an array of values
	 - a ['tableName'] (in case you want to test data definitions before binding)
	 - a ['commandType'] (eg. SELECT) which is useful for a PDO execution