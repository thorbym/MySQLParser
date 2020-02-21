<?php

require_once 'StringParser.php';

/**

Parses MySQL DELETE queries

*/

class DeleteParser extends StringParser {

	function processQuery($sql = false)
	{
		if (!$sql) {
			return false;
		}

		// find the existing where clause
		$oldWhereClause = $this->extractWhereClause($sql);

		// process the where clause to return an array of old=>new sql, and an array of colName=>vals to update
		$newWhereInfo = $this->processWhereClause($oldWhereClause);

		// rebuilds the old sql query using the new information
		$sql = $this->rebuildSqlQuery($sql, $newWhereInfo['sqlArray']);

		// return reconstructed sql, colName=>vals array and a tablename
		return [
			'sql' => $sql,
			'valuesArray' => $newWhereInfo['valuesArray'],
			'tableName' => $this->getTableName($sql, "DELETE"),
			'commandType' => "DELETE"
		];
	}

}

?>