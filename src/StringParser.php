<?php

/**

General cross-query MySQL string parsing functions

*/

class StringParser {

	// find the type of sql command being passed in (only accepts SELECT, UPDATE, DELETE and INSERT)
	function getCommandType($sql)
	{
		$commandTypeArr = ['SELECT', 'UPDATE', 'INSERT', 'DELETE'];
		if ($commandType = trim(substr($sql, 0, strpos($sql," ")))) {
			if (in_array($commandType, $commandTypeArr)) {
				return $commandType;
			}
			return false;
		}
	}

	function getStartAndEndPos($sql, $needle)
	{
		$pos = strpos($sql, $needle);
		if ($pos === false) {
			return false;
		}
		return [
			'startPos' => $pos,
			'endPos' => $pos + strlen($needle)
		];
	}

	function getFirstOperatorPos($sql)
	{
		$operatorArr = [];
		// loop through possible operators, to find which occurs first
		foreach (['GROUP BY','HAVING','ORDER BY','LIMIT'] as $operator) {
			if ($position = strpos($sql,$operator)) {
				$operatorArr[$operator] = $position;
			}
		}
		if (empty($operatorArr)) return false;
		return min($operatorArr);
	}

	function getTableName($sql,$commandType)
	{
		switch ($commandType) {
			case "SELECT":
			case "DELETE":
				$fromArr = $this->getStartAndEndPos($sql, "FROM");
				$whereArr = $this->getStartAndEndPos($sql, "WHERE");
				if ($whereArr) {
					return trim(substr($sql,$fromArr['endPos'], ($whereArr['startPos'] - $fromArr['endPos'])));
				} else {
					return trim(substr($sql, $fromArr['endPos'], (strlen($sql) - $fromArr['endPos'])));
				}
				break;

			case "UPDATE":
				$setArr = $this->getStartAndEndPos($sql, "SET");
				return trim(str_replace('UPDATE ', '', substr($sql, 0, $setArr['startPos'])));
				break;

			case "INSERT":
				$sqlLessInsert = str_replace('INSERT INTO ', '', $sql);
				return trim(substr($sqlLessInsert, 0, strpos($sqlLessInsert, " ")));
				break;

			default:
				return false;
		}
	}

	// extract the substring between WHERE and either the first operator (ie. GROUP BY, or LIMIT) or, if none, the end
	function extractWhereClause($sql)
	{
		// grab the sql query from the WHERE clause onwards
		$firstOperatorPos = $this->getFirstOperatorPos($sql);
		$wherePos = $this->getStartAndEndPos($sql, "WHERE");
		if (!$wherePos) {
			return false;
		}
		if (!$firstOperatorPos) {
			return trim(substr($sql, $wherePos['endPos']));
		} else {
			$whereClauseLength = ($firstOperatorPos - $wherePos['endPos']);
			return trim(substr($sql, $wherePos['endPos'], $whereClauseLength));
		}
	}

	// splits a where clause into an array by AND or OR
	function splitWhereClause($whereClause)
	{
		return explode("|", str_replace(['AND', 'OR'], '|', $whereClause));
	}

	// checks all possible evaluators, and returns an array with start and end positions, plus evaluator name
	function getEvaluatorInfo($param)
	{
		$evaluators = ['>=', '<=', '>', '<', '!=', '=', 'NOT IN', 'IN', 'LIKE'];
		foreach ($evaluators as $evaluator) {
			if ($evalPos = strpos($param, $evaluator)) {
				$returnArr = $this->getStartAndEndPos($param, $evaluator);
				$returnArr['evaluator'] = $evaluator;
				return $returnArr;
			}
		}
		return false;
	}

	// takes where clause, splits into chunks, replaces the values with "?" and returns both the sql and the values
	function processWhereClause($whereClause)
	{
		// now we have the where clause, we need to split it into vars and vals
		$splitWhereClause = $this->splitWhereClause($whereClause);
		$valuesArray = [];
		$sqlArray = [];
		foreach ($splitWhereClause as $param) {
			$param = $this->removeNonMatchingBrackets($param);
			$evaluatorInfo = $this->getEvaluatorInfo($param);
			$processedParam = $this->processParam($param, $evaluatorInfo);
			$sqlArray[$param] = $processedParam['sqlString'];
			$valuesArray += $processedParam['valuesArray'];
		}
		return [
			'sqlArray' => $sqlArray,
			'valuesArray' => $valuesArray
		];
	}

	function processParam($param, $evaluatorInfo)
	{
		if (!$evaluatorInfo) {
			return [
				'sqlString' => false,
				'valuesArray' => []
			];
		}
		$evaluatorStartPos = $evaluatorInfo['startPos'];
		$evaluatorEndPos = $evaluatorInfo['endPos'];
		$evaluator = $evaluatorInfo['evaluator'];
		$colName = trim(substr($param, 0, $evaluatorStartPos));
		$newSqlString = $colName." ".$evaluator." ";
		$valString = trim(substr($param, $evaluatorEndPos));
		$valuesArray = [];
		if ($evaluator == "IN") {
			$inComma = false;
			$newSqlString .= "(";
			$vals = explode(',', substr($valString, 1, -1));
			foreach ($vals as $val) {
				$val = $this->processVal($val);
				$newSqlString .= $inComma."?";
				$inComma = ",";
				$valuesArray[$val] = $colName;
			}
			$newSqlString .= ")";
		} else {
			$val = $this->processVal($valString);
			$newSqlString .= "?";
			$valuesArray[$val] = $colName;
		}
		return [
			'sqlString' => $newSqlString,
			'valuesArray' => $valuesArray
		];
	}

	function processVal($val)
	{
		$val = trim($val);
		if (substr($val, 0, 1) == "'" || substr($val, 0, 1) == '"') {
			$val = substr($val,  1, -1);
		}
		if (is_numeric($val)) {
			$val = (int)$val;
		}
		return $val;
	}

	// find and remove non matching brackets - pretty dumb function that only has power to remove brackets from start and end of string
	function removeNonMatchingBrackets($param)
	{
		$param = trim($param);
		$openingBracketsCount = substr_count($param, '(');
		$closingBracketsCount = substr_count($param, ')');
		if ($openingBracketsCount != $closingBracketsCount) {
			if ($openingBracketsCount > $closingBracketsCount && substr($param, 0, 1) == '(') {
				$param = ltrim($param, '(');
			} elseif ($openingBracketsCount < $closingBracketsCount && substr($param, -1) == ')') {
				$param = rtrim($param, ')');
			}
		}
		return trim($param);
	}

	function rebuildSqlQuery($sql, $newSqlArray)
	{
		foreach ($newSqlArray as $oldSql=>$newSql) {
			if ($newSql) $sql = str_replace($oldSql, $newSql, $sql);
		}
		return $sql;
	}

}

?>