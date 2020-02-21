<?php

require_once 'StringParser.php';

/**

Parses MySQL INSERT INTO queries

*/

class InsertParser extends StringParser {

	var $tableName;

	function processQuery($sql=false)
	{
		if (!$sql) {
			return false;
		}

		if (strpos($sql, 'VALUES') === false) {
			return false;
		}

		// find the cols and vals from the insert string
		$colsString = $this->extractInsertCols($sql);
		$valsString = $this->extractInsertVals($sql);

		// process the SET info to return an array of old => new sql, and an array of colName => vals to SET
		$newSetInfo = $this->processSetInfo($setInfo);
		$this->sqlArray += $newSetInfo['sqlArray'];
		$this->valuesArray += $newSetInfo['valuesArray'];

		// find the existing where clause
		$oldWhereClause = $this->extractWhereClause($sql);

		// process the where clause to return an array of old => new sql, and an array of colName => vals to update
		$newWhereInfo = $this->processWhereClause($oldWhereClause);
		$this->sqlArray += $newWhereInfo['sqlArray'];
		$this->valuesArray += $newWhereInfo['valuesArray'];

		// rebuilds the old sql query using the new information
		$sql = $this->rebuildSqlQuery($sql, $this->sqlArray);

		// return reconstructed sql, colName => vals array and a tablename
		return [
			'sql' => $sql,
			'valuesArray' => $this->valuesArray,
			'tableName' => $this->tableName,
			'commandType'=>"INSERT"
		];
	}

	// extract the substring between tableName and either the VALUES
	function extractInsertCols($sql)
	{
		$this->tableName = $this->getTableName($sql, "INSERT");
		$tableNamePos = $this->getStartAndEndPos($sql, $this->tableName);
		$valuesPos = $this->getStartAndEndPos($sql, "VALUES");
		$insertColsLength = ($valuesPos['startPos'] - $tableNamePos['endPos']);
		return trim(substr($sql, $tableNamePos['endPos'], $insertColsLength));
	}

	// extract the substring of the VALUES
	function extractInsertVals($sql)
	{
		$valuesPos = $this->getStartAndEndPos($sql, "VALUES");
		return trim(substr($sql, $valuesPos['endPos']));
	}

}

?>