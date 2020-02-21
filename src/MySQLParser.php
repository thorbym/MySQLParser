<?php

require_once 'StringParser.php';
require_once 'SelectParser.php';
require_once 'UpdateParser.php';
require_once 'InsertParser.php';
require_once 'DeleteParser.php';

class MySQLParser extends StringParser {

	function __construct($sql = false) {
		if ($sql) {
			$this->processSqlQuery($sql);
		}
	}

	function processSqlQuery($sql = false)
	{
		if (!$sql) {
			return false;
		}

		$commandType = $this->getCommandType($sql);

		if ($commandType) {

			switch ($commandType) {
				case 'SELECT':
					$parser = new SelectParser();
					break;
				case 'UPDATE':
					$parser = new UpdateParser();
					break;
				case 'INSERT':
					$parser = new InsertParser();
					break;
				case 'DELETE':
					$parser = new DeleteParser();
					break;
				default:
					return false;
			}

			return $parser->processQuery($sql);
		}
		return false;
	}

}

?>