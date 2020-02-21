<?php

require_once '../../src/MySQLParser.php';
require_once '../../src/UpdateParser.php';
require_once '../../src/InsertParser.php';

class SqlParserTest extends \PHPUnit\Framework\TestCase {

	var $beforeArray = [
		'SELECT' => "SELECT * FROM customers WHERE (customerID IN (1,2.3) OR firstName LIKE 'S%') AND dateOfBirth LIKE '19%' AND firstName IS NOT NULL ORDER BY customerID DESC LIMIT 2",
		'UPDATE' => "UPDATE customers SET customerID=2, firstName='Bucket' WHERE lastName='Sunday' AND firstName IS NOT NULL AND customerID=1",
		'DELETE' => "DELETE FROM customers WHERE customerID=2 AND lastName='Sunday'",
		'INSERT' => "INSERT INTO customers (firstName,lastName,age) VALUES ('Solomon', 'Grundy', 41)",
		false => "ALTER TABLE customers ADD COLUMN bum INT(1)"
	];
	var $afterArray = [
		'SELECT' => "SELECT * FROM customers WHERE (customerID IN (?,?) OR firstName LIKE ?) AND dateOfBirth LIKE ? AND firstName IS NOT NULL ORDER BY customerID DESC LIMIT 2",
		'UPDATE' => "UPDATE customers SET customerID=?, firstName=? WHERE lastName = ? AND firstName IS NOT NULL AND customerID = ?",
		'DELETE' => "DELETE FROM customers WHERE customerID = ? AND lastName = ?",
		'INSERT' => "INSERT INTO customers (firstName,lastName,age) VALUES (?,?,?)"
	];
	var $postParseValuesArraySelect = [
		1 => 'customerID',
		2.3 => 'customerID',
		'S%' => 'firstName',
		'19%' => 'dateOfBirth'
	];

	var $postParseValuesArrayUpdate = [
		2 => 'customerID',
		'Bucket' => 'firstName',
		'Sunday' => 'lastName',
		1 => 'customerID'
	];

	var $postParseValuesArrayDelete = [
		'Sunday' => 'lastName',
		2 =>'customerID'
	];

	///////////////////////////
	// INSERT FUNCTION TESTS //
	///////////////////////////
/*
	public function testWholeInsertSqlParser()
	{

	}
*/

	public function testExtractInsertCols()
	{
		$sqlParser = new InsertParser();
		$this->assertEquals($sqlParser->extractInsertCols($this->beforeArray['INSERT']), "(firstName,lastName,age)");
	}

	public function testExtractInsertVals()
	{
		$sqlParser = new InsertParser();
		$this->assertEquals($sqlParser->extractInsertVals($this->beforeArray['INSERT']), "('Solomon', 'Grundy', 41)");
	}

	///////////////////////////
	// DELETE FUNCTION TESTS //
	///////////////////////////

	public function testWholeDeleteSqlParser()
	{
		$sqlParser = new SqlParser();
		$returnArr = $sqlParser->processSqlQuery($this->beforeArray['DELETE']);
		$this->assertEquals($returnArr['sql'], $this->afterArray['DELETE']);
		$this->assertEquals($returnArr['valuesArray'], $this->postParseValuesArrayDelete);
		$this->assertEquals($returnArr['tableName'], 'customers');
	}	

	///////////////////////////
	// UPDATE FUNCTION TESTS //
	///////////////////////////

	public function testWholeUpdateSqlParser()
	{
		$sqlParser = new SqlParser();
		$returnArr = $sqlParser->processSqlQuery($this->beforeArray['UPDATE']);
		$this->assertEquals($returnArr['sql'], $this->afterArray['UPDATE']);
		$this->assertEquals($returnArr['valuesArray'], $this->postParseValuesArrayUpdate);
		$this->assertEquals($returnArr['tableName'], 'customers');
	}

	// NOTE TODO!! swapping array to be val=>colName won't work either IF the val is same - ie. customerID=2 AND numberOfChildren=2
	// ALSO - what if we do UPDATE table SET title=CONCAT(title,'arses') or some such?

	public function testExtractSetInfo()
	{
		$sqlParser = new UpdateParser();
		$this->assertEquals($sqlParser->extractSetInfo($this->beforeArray['UPDATE']), "customerID=2, firstName='Bucket'");
	}

	public function testProcessSetInfo()
	{
		$sqlParser = new UpdateParser();
		$setInfo = "customerID=2, firstName='Bucket'";
		$setInfoArray = $sqlParser->processSetInfo($setInfo);
		$this->assertEquals($setInfoArray['sqlArray'], ["customerID=2"=>"customerID=?", "firstName='Bucket'"=>"firstName=?"]);
		$this->assertEquals($setInfoArray['valuesArray'], ["2"=>"customerID", "Bucket"=>"firstName"]);
	}

	///////////////////////////
	// SELECT FUNCTION TESTS //
	///////////////////////////

	public function testWholeSelectSqlParser()
	{
		$sqlParser = new SqlParser();
		$returnArr = $sqlParser->processSqlQuery($this->beforeArray['SELECT']);
		$this->assertEquals($returnArr['sql'], $this->afterArray['SELECT']);
		$this->assertEquals($returnArr['valuesArray'], $this->postParseValuesArraySelect);
		$this->assertEquals($returnArr['tableName'], 'customers');
	}

	///////////////////////////////////
	// STRING PARSING FUNCTION TESTS //
	///////////////////////////////////

	public function testGetCommandType()
	{
		$sqlParser = new SqlParser();
		foreach ($this->beforeArray as $commandType=>$sql) {
			$this->assertEquals($sqlParser->getCommandType($sql), $commandType);
		}
	}

	public function testGetStartAndEndPos()
	{
		$sqlParser = new SqlParser();
		$posArr = $sqlParser->getStartAndEndPos($this->beforeArray['SELECT'], "WHERE");
		$this->assertEquals($posArr['startPos'], 24);
		$this->assertEquals($posArr['endPos'], 29);
	}

	public function testGetFirstOperatorPos()
	{
		$sqlParser = new SqlParser();
		$this->assertEquals($sqlParser->getFirstOperatorPos($this->beforeArray['SELECT']), 130);
	}

	public function testGetTableNameFromSelectQuery($commandType = "SELECT")
	{
		$sqlParser = new SqlParser();
		$this->assertEquals($sqlParser->getTableName($this->beforeArray[$commandType], $commandType), 'customers');
	}

	public function testGetTableNameFromUpdateQuery($commandType = "UPDATE")
	{
		$sqlParser = new SqlParser();
		$this->assertEquals($sqlParser->getTableName($this->beforeArray[$commandType], $commandType), 'customers');
	}

	public function testGetTableNameFromDeleteQuery($commandType = "DELETE")
	{
		$sqlParser = new SqlParser();
		$this->assertEquals($sqlParser->getTableName($this->beforeArray[$commandType], $commandType), 'customers');
	}

	public function testGetTableNameFromInsertQuery($commandType = "INSERT")
	{
		$sqlParser = new SqlParser();
		$this->assertEquals($sqlParser->getTableName($this->beforeArray[$commandType], $commandType), 'customers');
	}

	public function testExtractWhereClause()
	{
		$sqlParser = new SqlParser();
		$shouldBeWhereClause = "(customerID IN (1,2.3) OR firstName LIKE 'S%') AND dateOfBirth LIKE '19%' AND firstName IS NOT NULL";
		$this->assertEquals($sqlParser->extractWhereClause($this->beforeArray['SELECT']), $shouldBeWhereClause);
	}

	public function testSplitWhereClause()
	{
		$sqlParser = new Sqlparser();
		$whereClause = $sqlParser->extractWhereClause($this->beforeArray['SELECT']);
		// note that the shouldBeSplitWhereClause array has spaces in the text, as the sql will explode in this way
		$shouldBeSplitWhereClause = ["(customerID IN (1,2.3) "," firstName LIKE 'S%') "," dateOfBirth LIKE '19%' "," firstName IS NOT NULL"];
		$this->assertEquals($sqlParser->splitWhereClause($whereClause), $shouldBeSplitWhereClause);
	}

	public function testRemoveNonMatchingBrackets()
	{
		$paramsArr = ["(customerID IN (1,2.3)"=>"customerID IN (1,2.3)", "firstName LIKE 'S%')"=>"firstName LIKE 'S%'"];
		foreach ($paramsArr as $oldSql=>$newSql) {
			$sqlParser = new SqlParser();
			$this->assertEquals($sqlParser->removeNonMatchingBrackets($oldSql), $newSql);
		}
	}

	public function testGetEvaluatorInfo()
	{
		$param = "dateOfBirth LIKE '19%'";
		$shouldBeEvaluatorInfo = ['startPos'=>12, 'endPos'=>16, 'evaluator'=>'LIKE'];
		$sqlParser = new Sqlparser();
		$this->assertEquals($sqlParser->getEvaluatorInfo($param), $shouldBeEvaluatorInfo);
	}

	public function testProcessParam()
	{
		$sqlParser = new SqlParser();
		$paramArr = [
			"customerID IN (1,2.3)"=>['sqlString'=>"customerID IN (?,?)", 'valuesArray'=>[1=>'customerID', 2.3=>'customerID']],
			"firstName LIKE 'S%'"=>['sqlString'=>"firstName LIKE ?", 'valuesArray'=>['S%'=>'firstName']],
			"dateOfBirth LIKE '19%'"=>['sqlString'=>'dateOfBirth LIKE ?', 'valuesArray'=>['19%'=>'dateOfBirth']],
			"firstName IS NOT NULL"=>['sqlString'=>false, 'valuesArray'=>[]]
		];
		foreach ($paramArr as $param=>$info) {
			$evaluatorInfo = $sqlParser->getEvaluatorInfo($param);
			$processedParam = $sqlParser->processParam($param, $evaluatorInfo);
			$this->assertEquals($processedParam['sqlString'], $info['sqlString']);
			$this->assertEquals($processedParam['valuesArray'], $info['valuesArray']);
		}
	}

}

?>