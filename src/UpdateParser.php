<?php

require_once 'StringParser.php';

/**

Parses MySQL UPDATE queries

*/

class UpdateParser extends StringParser {

	function processQuery($sql=false)
	{
		if (!$sql) return false;

		// find the SET colNames = vals
		$setInfo = $this->extractSetInfo($sql);

		// process the SET info to return an array of old => new sql, and an array of colName => vals to SET
		$newSetInfo = $this->processSetInfo($setInfo);
		$sqlArray = $newSetInfo['sqlArray'];
		$valuesArray = $newSetInfo['valuesArray'];

		// find the existing where clause
		$oldWhereClause = $this->extractWhereClause($sql);

		// process the where clause to return an array of old=>new sql, and an array of colName=>vals to update
		$newWhereInfo = $this->processWhereClause($oldWhereClause);
		$sqlArray += $newWhereInfo['sqlArray'];
		$valuesArray += $newWhereInfo['valuesArray'];

		// rebuilds the old sql query using the new information
		$sql = $this->rebuildSqlQuery($sql, $sqlArray);

		// return reconstructed sql, colName=>vals array and a tablename
		return [
			'sql'=>$sql,
			'valuesArray'=>$this->valuesArray,
			'tableName'=>$this->getTableName($sql, "UPDATE"),
			'commandType'=>"UPDATE"
		];
	}

	// extract the substring between WHERE and either the WHERE or, if none, the end
	function extractSetInfo($sql)
	{
		$setPos = $this->getStartAndEndPos($sql, "SET");
		$wherePos = $this->getStartAndEndPos($sql, "WHERE");

		if (!$setPos) {
			return false;
		}

		if (!$wherePos) {
			return trim(substr($sql, $setPos['endPos']));
		} else {
			$setLength = ($wherePos['startPos'] - $setPos['endPos']);
			return trim(substr($sql, $setPos['endPos'], $setLength));
		}
	}

	function processSetInfo($setInfo) {
		// now we have the SET info, we need to split it into vars and vals
		$splitSetInfo = explode(",", $setInfo);
		$valuesArray = [];
		$sqlArray = [];

		foreach ($splitSetInfo as $param) {
			$param = trim($param);
			$equalPos = strpos($param, '=');
			$colName = trim(substr($param, 0, $equalPos));
			$val = substr($param, $equalPos+1);
			$val = $this->processVal($val);
			$sqlArray[$param] = $colName . "=?";
			$valuesArray[$val] = $colName;
		}

		return [
			'sqlArray' => $sqlArray,
			'valuesArray' => $valuesArray
		];
	}

}

?>