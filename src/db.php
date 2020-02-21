<?php

require_once('MySQLParser.php');

class DB extends MySQLParser {

	var $version="17.02.14";

	// Internal vars
	var $dbName='practiceManager';
	var $ddSrc="information_schema";
	var $username='root';
	var $password='';
	var $tables=false; // TABLES cache
	var $columns=false; // COLUMNS cache

	var $phpParser=false;
	var $mysqli=false;
	var $pdo=false;

	var $sql=false; // The current/last executed SQL statement
	var $startTime=0;
	
	var $server="127.0.0.1";
	var $charSet=false; // "utf8mb4";
	var $plural=false;

	function __construct() {
		session_name($this->dbName);
		session_start();
		session_set_cookie_params(88000); // 1 day cookie timeout
		session_cache_limiter('private, must-revalidate');
		session_cache_expire(60);
	}


	// ----------------
	// MYSQLi FUNCTIONS
	// ----------------


	// Open MySQLi connection (used for raw SQL SELECTS due to superior (associated array) returns over PDO)
	function openMysqli() {
		if ($this->mysqli) return true; // Already connected
		$this->closeMysqli(); // Ensure we are closed / a different connection is closed
		$this->mysqli=new mysqli($this->server, $this->username, $this->password, $this->dbName);
		if ($this->charSet) $this->mysqli->set_charset($this->charSet);
		if ($this->mysqli->connect_errno) {
			echo "KlikDB.openMysqli: connection to ".$this->server.":".$this->dbName." failed (" . $this->mysqli->connect_errno . ") " . $this->mysqli->connect_error;
		}
	}

	// Open PDO connection (used for INSERT/UPDATE due to superior (named parameter) prepared statement support over mysqli)
	function openPDO() {
		if ($this->pdo) return true; // Already connected
		try {
			$this->pdo=new PDO("mysql:".(($this->charSet)?"charset=".$this->charSet.";":"")."host=".$this->server.";dbname=".$this->dbName, $this->username, $this->password);
			$this->pdo->setAttribute(PDO::ATTR_ERRMODE,PDO::ERRMODE_EXCEPTION);
			$this->pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES,false);
		} catch(PDOException $e) {
			trace("KlikDB.openPDO:PDO connection failed:".$e->getMessage());
		}
	}

	function closeMysqli($force=0) {
	  	if ($this->mysqli) $this->mysqli->close();
		$this->mysqli=false;
	}

	function closePDO($force=0) {
		if (!$force) return false;
	  	if ($this->pdo) $this->pdo=false;
	}

	function close() {
		$this->closeMysqli(1);
		$this->closePdo(1);
	}

	function prepare($sql) {
		// Pass in a prepared statement. Will re-use an existing PDOStatement if already used this session
    	$this->sql=$sql;
		try { 
			$stmt=$this->pdo->prepare($sql); 
		} catch(Exception $e) {
			trace("KlikDB.prepare PDO err: ".$e->getMessage()." using [".$sql."]"); return false;
		}
		// $this->preparedStmts[$sql]=$stmt; // Cache me if you can
		return $stmt;
	}


	// ------------------------
	// DATA RETRIEVAL FUNCTIONS
	// ------------------------


	// Retrieve all results from the given SQL query.
	function getAll($sql) {
		$this->openMysqli();
		$res=$this->mysqli->query($sql);
  		if (!$res) return [];
  		if ($res->num_rows===0) return [];

    	$r=$res->fetch_all(MYSQLI_ASSOC);
  		$res->free();
  		$this->closeMysqli();
    	return $r;
	}

	function newGetAll($sql) {
		$sqlInfo = $this->processSqlQuery($sql);
		if (!$sqlInfo) return false;
    	$this->openPDO();
    	$stmt = $this->pdo->prepare($sqlInfo['sql']);
		if (!isset($this->columns[$sqlInfo['tableName']])) {
			$this->loadColumns($sqlInfo['tableName']);
		}
		if (isset($this->columns[$sqlInfo['tableName']])) {
			$colDefs = $this->columns[$sqlInfo['tableName']]['iCols'];
		} else {
			$colDefs = false;
		}
		$valuesArray = [];
		if (!empty($sqlInfo['valuesArray'])) {
			// Bind params
			$count = 0;
			foreach ($sqlInfo['valuesArray'] as $val => $colName) {
				$valuesArray[] = $val;
				$count++;
				if (isnull($val)) {
					$stmt->bindValue($count, NULL, PDO::PARAM_NULL);
				} else if (strpos($colDefs[$colName]['DATA_TYPE'], "int") !== false) {
	        		$stmt->bindValue($count, (int)$val, PDO::PARAM_INT);
				} else {
					$stmt->bindValue($count, $val, PDO::PARAM_STR);
				}
			}
		}
		$ok = false;
		try {
			$ok=$stmt->execute($valuesArray);
			if ($sqlInfo['commandType'] == "SELECT") {
				$returnArray = $stmt->fetchAll(\PDO::FETCH_ASSOC);
				if (empty($returnArray)) {
		    		return [];
		    	} else {
		    		return $returnArray;
		    	}
			} else {
				return $ok;
			}
		} catch(Exception $e) {
			trace("PDO err: ".$e->getMessage());
			$this->closePDO();
			return false;
		}
		if (!$ok) {
			trace("Failed to execute [".$sql."]");
			$this->closePDO();
			return false;
		}
	}

	function getRow($sql) {
		$this->openMysqli();
  		$res=$this->mysqli->query($sql);
  		if (!$res) {
	  		trace("KlikDB.GetRow:".$this->mysqli->error." [".$sql."]");
	  		return [];
	  	}
	  	if ($res->num_rows===0) return false;
		$res->data_seek(0); // Not needed?
	  	$r=$res->fetch_array(MYSQLI_ASSOC);
	  	$res->free();
	  	$this->closeMysqli();
	  	return $r;
	}

	// returns one value - $col can be either a numeric key or an associative key
	function getOne($sql,$col=0) {
		$ts=microtime(true);
		$this->openMysqli();
	  	$res=$this->mysqli->query($sql);
	  	if (!$res) {
	  		trace("KlikDB.GetRow:".$this->mysqli->error." [".$sql."]");
	  		return [];
	  	}
  		if ($res->num_rows===0) return false;
		$res->data_seek(0); // Not needed?
		if (is_numeric($col)) {
    		$row=$res->fetch_array(MYSQLI_NUM);
		} else {
			$row=$res->fetch_array(MYSQLI_ASSOC);
		}
		return ($row)?$row[$col]:false;
	}

	// Returns every value of one column from the given SQL as a comma separated string
	function getKeys($sql,$col=0) {
		return crushOut($this->getAll($sql),$col);
	}

	// Retrieve key e.g. for the row we just inserted
	function retrieveKey($tableName,$pairs,$keyCol=false,$minMax='MAX') {
		if (!$keyCol) {
			$keyCol=$this->getKeyCol($tableName);
		}
		// if (isset($pairs[$keyCol])) return $pairs[$keyCol];
		$comma=""; $and=""; $data=[];
		$sql="SELECT ".$minMax."(".$keyCol.") FROM ".$tableName." WHERE ";
		foreach ($pairs as $col=>$val) {
			$colDef=$this->getColFromDD($tableName,$col);
		  	if ($col!=$keyCol && $val!==false && $val!==null) {
		  		// Don't include null strings, as these may have defaults set (particularly dates, which will be on the db as 0000-00-00 00:00:00)
		        if ($val==='NULL') {
					$sql.=$and.$col." IS NULL";
		        } else {
					$sql.=$and.$col."=".$this->formatForColDef($val,$colDef,true);
					$data[$col]=$val;
		        }
		        $and=" AND ";
			}
		}
		// Use mysqli for this, as we may have a connection already
		return $this->GetOne($sql);
	}

	// Looks for a single existing database row that matches the contents of arr (or only those columns specified in matchCols)
	function findMatch($tableName,$arr,$matchFields=false,$ignoreFields=false) {
	    if (!($matchFields)) $matchFields=crushOut($arr,-1);
	    $matchFields=iExplode($matchFields);
	  	if ($ignoreFields) {
	  		foreach (explode(',',$ignoreFields) as $f) {
	  			unset($matchFields[$f]);
	  		}
	  	}
		$keyCol=$this->getKeyCol($tableName);
		$matched=true;
		$arr[$keyCol]=0;
    	// Look for a matching existing row, testing equivalence against the matchCols
		$colMissing=false;
		$where=""; $and="";
		foreach ($matchFields as $colName) {
			if (!isset($arr[$colName])) {
				$colMissing=true;
			} else {
				$val=$this->formatForCol($arr[$colName],$tableName,$colName,true);
				$where.=$and.$colName.(($val===false)?" IS NULL":"=".$val); $and=" AND ";
			}
		}
		if (notnull($where)) { // Does the data map to an existing row?
			$keyVal=$this->GetOne("SELECT ".$keyCol." FROM ".$tableName." WHERE ".$where);
			$arr[$keyCol]=($keyVal)?$keyVal:0;
		}
		return $arr[$keyCol];
	}

	function getArray($tableName,$extraParams=false,$indexOnPrimaryKey=false) {
		$sql = "SELECT * FROM ".$tableName;
		// check for businessID in this table
		$exists = $this->columnExists($tableName,'businessID');
		$where=false;
		if ($exists) {
			$where=true;
			// then 1) make sure user is restricted to only their validBusinessIDs
			$sql.=" WHERE businessID IN (".$_SESSION['validBusinessIDs'].")";
			// and 2) only return info for the currentBusiness
			if ($tableName!='businesses') $sql.=" AND businessID=".$_SESSION['currentBusiness']['businessID'];
		}
		if ($extraParams) {
			// can restrict returned dataset by passing extra params
			$extraParamsSql=false;
			foreach ($extraParams as $col=>$val) {
				$colDef=$this->getColFromDD($tableName,$col);
				$extraParamsSql.=(($where)?" AND ":" WHERE ");
				$extraParamsSql.=$col."=".$this->formatForColDef($val,$colDef,true);
				$where=true;
			}
			$sql.=$extraParamsSql;
		}
		if ($indexOnPrimaryKey) {
			// instead of zero indexed, index the array on primary key
			$returnArr=[];
			$pk=$this->getKeyCol($tableName);
			foreach ($this->GetAll($sql) as $arr) {
				$returnArr[$arr[$pk]]=$arr;
			}
		} else {
			$returnArr=$this->GetAll($sql);
		}
		return $returnArr;
	}

	function getSelectBoxData($tableName) {
		$foreignKeys = $this->getForeignKeys($tableName);
		$data = [];
		foreach ($foreignKeys as $fk=>$table) {
			if ($fk != "addressID") {
				$array = $this->getArray($table,false,true);
				if (!$array) continue;
				$data[$fk] = $array;
				$data[$fk]['pkName']=$fk;
				$data[$fk]['tableName']=$table;				
			}
		}
		if (empty($data)) return false;
		return $data;
	}

	function makeBusinessSelect() {

		$arr=$this->GetAll("SELECT * FROM businesses WHERE businessID IN (".$_SESSION['validBusinessIDs'].")");
		$selectBox="
			<div class='businessSelect'>
				<select name='validBusinessIDs' id='validBusinessIDs' onChange='setCurrentBusiness($(this).val());' >";
				if ($arr) {
					foreach ($arr as $a) {
						$selectBox.="<option value='".$a['businessID']."'".(($_SESSION['currentBusiness']['businessID']==$a['businessID'])?"selected='selected'":"").">".$a['name']."</option>";
					}
				}
				$selectBox.="
				</select>
			</div>
		";

		return $selectBox;
	}


	// --------------------------
	// DATABASE WRITING FUNCTIONS
	// --------------------------


	function doUpdate($tableName,$pairs,$keyVal=false,$keyCol=false) {
		if (!$keyCol) $keyCol=$this->getKeyCol($tableName);
		if (!$keyVal) $keyVal=(isset($pairs[$keyCol]))?$pairs[$keyCol]:false;
		if (!$keyVal) return false;
		if (!$pairs) return false; // Nothing to update
		$ts=microtime(true);
		$keyVal=(int)$keyVal;
		// log the change before the existing data changes
		$this->logChange($tableName,'U',$keyCol,$keyVal,$pairs);
		// Build prepared statement
		$comma=$logComma=$logDesc=false;
		$sql="UPDATE ".$tableName." SET ";
		foreach ($pairs as $col=>$val) {
			if ($col!=$keyCol) {
				$colDef=$this->getColFromDD($tableName,$col); // Check column exists
				if ($colDef) {
					$sql.=$comma.$col."=:".$col; $comma=",";
				}
			}
		}
		$sql.=" WHERE ".$keyCol."=:".$keyCol;

		$this->openPDO();
		$stmt=$this->prepare($sql);
		// Bind data
		foreach ($pairs as $col=>$val) {
			if ($col!=$keyCol) {
				$colDef=$this->getColFromDD($tableName,$col); // Check column exists
				if ($colDef) {
					if (isnull($val)) {
						$stmt->bindValue(':'.$col, $val, PDO::PARAM_NULL);
					} else if (in($colDef['DATA_TYPE'],"int,bigint,mediumint,smallint,tinyint")) {
						$stmt->bindValue(':'.$col, (int)$val, PDO::PARAM_INT);
					} else {
						$stmt->bindValue(':'.$col, $val, PDO::PARAM_STR);
					}
				}
			}
		}
		$stmt->bindValue(':'.$keyCol, (int)$keyVal, PDO::PARAM_INT);
		if (!$stmt) { $this->closePDO(); return false; }
		try { $ok=$stmt->execute(); } catch(Exception $e) { trace("KlikDB.doUpdate PDO err: ".$e->getMessage()); $this->closePDO(); return false; }
		return $keyVal;
	}

	function doInsert($tableName,$valuesArr=false,$allowPK=false) {
		if (!is_array($valuesArr) || sizeOf($valuesArr)<1) return false;
		$keyCol=$this->getKeyCol($tableName);
		if (!isset($this->columns[$tableName])) $this->loadColumns($tableName);
		$colDefs=(isset($this->columns[$tableName]))?$this->columns[$tableName]['iCols']:false;
		// Grab the valid table columns
		if (!$colDefs) return false;
		$ts=microtime(true);
		$sqlA=$sqlB=$questionMarks=$comma=$logDesc=false;
		$cols=[]; $i=0;
		foreach ($valuesArr as $col=>$val) {
			if (($col!=$keyCol || $allowPK) && isset($colDefs[$col])) {
				$cols[$i++]=$col;
				$sqlA.=(($sqlA)?",":"").$col;
				$questionMarks.=(($questionMarks)?",":"")."?";
			}
		}

		// Build prepared statement
		$sql="INSERT INTO ".$tableName." (".$sqlA.") VALUES (".$questionMarks.")";

		$this->openPDO();
		$stmt=$this->prepare($sql); // Re-use previous prepared statements if poss

		// Bind params
		$count=0;
		foreach ($valuesArr as $col=>$val) {
			$count++;
			if (isnull($val)) {
				$stmt->bindValue($count, NULL, PDO::PARAM_NULL);
			} else if (strpos($colDefs[$col]['DATA_TYPE'], "int") !== false) {
        		$stmt->bindValue($count, (int)$val, PDO::PARAM_INT);
			} else {
				$stmt->bindValue($count, $val, PDO::PARAM_STR);
			}
		}

		$ok=false;
		if (!$stmt) {
			trace("Failed to create PDO stmt on ".$tableName." [".$sql."]!");
			$this->closePDO();
			return false;
		}
		try {
			$ok=$stmt->execute();
		} catch(Exception $e) {
			trace("KlikDB.doInsert PDO err: ".$e->getMessage());
			$this->closePDO();
			return false;
		}
		if (!$ok) {
			trace("Failed to execute stmt for [".$sql."]");
			$this->closePDO();
			return false;
		}
		$keyVal=$this->pdo->lastInsertId();
		if (!($keyVal)) {
			// Weird bug where pdo occasionally returns 0 here even though record created
			$keyVal=$this->retrieveKey($tableName,$pairs,$keyCol,'MAX');
			if (!$keyVal) {
				trace("PDO failed to retrieve lastInsertId, as did retrieveKey() for ".$tableName.". [Retrieve SQL: ".$this->sql."]");
				$this->closePDO();
				return false;
			}
		}
    	$this->closePDO();
    	$this->logChange($tableName,'I',$keyCol,$keyVal,$valuesArr);
		return $keyVal;
	}

	// Delete specific row using prepared statement
	function doDelete($tableName,$keyVal) {
		$keyCol=$this->getKeyCol($tableName);
		$sql="DELETE FROM ".$tableName." WHERE ".$keyCol."=:".$keyCol;
		$this->openPDO();
		$stmt=$this->prepare($sql);
		$stmt->bindValue(1, (int)$keyVal, PDO::PARAM_INT);
		try {
			$ok=$stmt->execute();
    		$this->closePDO();
    		$this->logChange($tableName,"D",$keyCol,$keyVal);
    		return $keyVal;
		} catch(Exception $e) {
			trace("DB.doDelete PDO err: ".$e->getMessage());
			$this->closePDO();
			return false;
		}
	}

	// creates a logged record of every entry in, or change on, the database
	function logChange($tableName,$type,$keyCol,$keyVal,$pairsArr=false) {

		if ($tableName=="log") return false;

		$logDesc=false;
		switch ($type) {

			// insert
			case 'I':
				if (!$pairsArr) return false;
			    foreach ($pairsArr as $col=>$val) {
			    	$logDesc.=(($logDesc)?",":"").'"'.$col.'":"'.$val.'"';
			    }
				break;

			// delete
			case 'D':
				$existing=$this->getRow("SELECT * FROM ".$tableName." WHERE ".$keyCol."=".$keyVal);
				foreach ($existing as $col=>$val) {
					$logDesc.=(($logDesc)?",":"").'"'.$col.'":"'.$val.'"';
				}
				break;

			// update
			case 'U':
				if (!$pairsArr) return false;
				$existing=$this->getRow("SELECT * FROM ".$tableName." WHERE ".$keyCol."=".$keyVal);
				foreach ($pairsArr as $col=>$val) {
					if ($existing[$col] != $val) $logDesc.=(($logDesc)?",":"").'"'.$col.'":"'.$existing[$col].'"';
				}

				break;

			default:
				return false;

		}

		$log=$this->doInsert(
			'log',[
				'businessID'=>$_SESSION['currentBusiness']['businessID'],
				'userID'=>$_SESSION['userID'],
				'tableName'=>$tableName,
				'keyCol'=>$keyCol,
				'keyVal'=>$keyVal,
				'action'=>$type,
				'info'=>"{".$logDesc."}"
			]
		);
	}

	// Inserts a table row with information from arr, or updates if PK included
	// Ignores data with no corresponding column (unlike using doUpdate/doInsert directly)
	// Returns primary key
	function writeArray($tableName,$arr,$skipNulls=false) {
		$keyCol=$this->getKeyCol($tableName);
		$keyVal=(isset($arr[$keyCol]))?(int)$arr[$keyCol]:0;
		// Build up an array of formatted pairs
		$pairs=$this->formatPairs($tableName,$arr,$skipNulls);
		return ($keyVal==0)?$this->doInsert($tableName,$pairs):$this->doUpdate($tableName,$pairs,$keyVal,$keyCol);
	}
/*
		if ($tableName == "appointments") {
			// we are adding or changing an appointment, need to create a message to update the relevant employee
			$customerName = $DB->getOne("SELECT CONCAT(firstName,' ',lastName) FROM customers WHERE customerID=".p('customerID'));
			if ($appointmentID = p('appointmentID')) {
				// changed appointment
				$previousAppointment = $DB->GetRow("SELECT * FROM appointments WHERE appointmentID=".$appointmentID);
				$message = "Appointment changed: ".$customerName." moved from ".$previousAppointment['startTime']." - ".$previousAppointment['endTime']." on ".convertDate($previousAppointment['date'])." to ".p('startTime')." - ".p('endTime')." on ".convertDate(p('date'));
			} else {
				// new appointment
				$message = "New appointment: ".$customerName.", ".p('startTime')." - ".p('endTime')." on ".convertDate(p('date'));
			}
			$messageID = $DB->writeArray("messages",['firstName'=>'Auto','lastName'=>'message','message'=>$message]);
			$DB->writeArray("messageUsers",['messageID'=>$messageID,'userID'=>1]);
		}

*/
	// Performs a writeArray, but if the PK is not set it first looks for an existing record based on matchCols - a comma separated list of columns whose data that must match for a row to be considered equivalent
	// A match on data in all matchCols (essentially a "whatMakesARecordUnique") causes DB->mergeData() to UPDATE rather than INSERT.
	// Essentially an array-based upsert. Pass unique=true to enforce that only one of these records exists
	// e.g. mergeData("products",array('productID'=>0,productCode=>'MG001',title='Mini-gun',price=10.00),"productCode,title"); will look for a product with productCode MG001 and title Mini-gun and update the matching row if it finds it
	function mergeData($tableName,$arr,$matchFields=false,$unique=false) {
		$keyCol=$this->getKeyCol($tableName);
		$arr[$keyCol]=$this->findMatch($tableName,$arr,$matchFields);
		// if (unique) keyVal=stripToOneRow(buildWhere(arr,matchCols),keyVal)
		return $this->writeArray($tableName,$arr);
	}


	// -------------------------
	// DATA DICTIONARY FUNCTIONS
	// -------------------------


	// Takes a column definition and formats a value for it
	function formatForColDef($val, $colDef) {
  		if ((is_array($val) && sizeOf($val)==0) || isnull($val)) return null;
		$colType=$colDef['DATA_TYPE'];
		if (in($colType,"char,longtext,mediumtext,text,varchar,date,datetime,timestamp")) {
      		if ($colType != "longtext") {
				$val=(in($colType,"date,datetime,timestamp"))?convertDate($val,"-"):substr($val,0,$colDef['CHARACTER_MAXIMUM_LENGTH']);
      		}
			return $val;
		} else if (in($colType,"int,bigint,decimal,double,mediumint,smallint,tinyint")) {
			$val=castAsNum($val);
		}
		return $val;
	}

	// Coder-friendly version of formatForColType, accepting table and column name
	function formatForCol($val,$tableName,$colName,$quoteStrings=false) {
		return $this->formatForColDef($val,$this->getColFromDD($tableName,$colName), $quoteStrings);
	}

	// Format an array of key=>value pairs depending on column definition, stripping out weirdos
  function formatPairs($tableName,$arr,$skipNulls=false) {
		$pairs=[];
		$cols=$this->getColsFromDD($tableName);
		if (!$cols) return false;
		foreach ($cols as $colDef) { // Note: loop over table cols (rather than passed data) to shed crap columns
			$col=$colDef['COLUMN_NAME'];
			$val=(isset($arr[$col]))?$arr[$col]:false;
			if ($val!==false && (!$skipNulls || notnull($val) || $val===0)) {
        		$pairs[$col]=$this->formatForColDef($val,$colDef);
      		}
		}
		return $pairs;
	}

	// Searches for a key in table that matches the values given in the key=>value array
  	function search($tableName,$arr,$skipNulls=false,$minMax='MIN') {
		$keyCol=$this->getKeyCol($tableName);
		if (nvl(getIfSet($arr,$keyCol),0)>0) { return $arr[$keyCol]; } // Primary key in arr
		$pairs=$this->formatPairs($tableName,$arr,$skipNulls);
		if (!$pairs) return false;
		$keyVal=$this->retrieveKey($tableName,$pairs,$keyCol,$minMax);
		return $keyVal;
	}

	function getTablesFromDD($forceCacheReset=0) {
		if ($this->tables) return $this->tables;
		// Temporarily connect to the information_schema DB
		$this->openMysqli($this->ddSrc);
		// Get the tables (first session to encounter this does the work for everyone)
		$this->tables=$this->GetAll("SELECT TABLE_NAME,ENGINE,TABLE_ROWS,AUTO_INCREMENT,DATA_LENGTH FROM TABLES WHERE TABLE_SCHEMA='".$this->dbName."' ORDER BY TABLE_NAME",1);
		// This is essential, so also alert the user (probably a developer if things are this un-setup)
		if (!$this->tables) {
			echo "<p>KlikDB.getTablesFromDD: Fatal error - no access to information_schema</p>";
			$this->closeMysqli(1);
			return false;
		}
		if ($this->ddSrc=="information_schema" || !$this->persistent) $this->closeMysqli(1);
		return $this->tables;
	}

	function loadColumns($tableName,$suppressComplaints=true) {
		if (isset($this->columns[$tableName])) return false;
		// Load from information_schema (the first session to do this will save it in memcache for all others)
		$sql="SELECT COLUMN_NAME,COLUMN_DEFAULT,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,COLUMN_TYPE,COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='".$tableName."' AND TABLE_SCHEMA='".$this->dbName."' ORDER BY ordinal_position";

		$this->openMysqli($this->ddSrc);
		$res=$this->mysqli->query($sql);
		if (!$res || $res->num_rows<1) {
			if (!$res) echo "<p>KlikDB.loadColumns: Fatal error - no access to ".$this->ddSrc.".COLUMNS table:".$this->mysqli->error."</p>";
    		if ($res->num_rows<1 && !$suppressComplaints) trace("KlikDB.loadColumns:".$tableName." ".$this->ddSrc.".COLUMNS is empty");
			if ($this->ddSrc=="information_schema" || !$this->persistent) $this->closeMysqli(1);
			return false;
		}
		$this->columns[$tableName]=['rawCols'=>[],'iCols'=>[],'pk'=>false]; // Create entry for table whatever, so that we don't repeatedly attempt this if not found
		$count=$res->num_rows;
		$result=$res->fetch_all(MYSQLI_ASSOC);

		$res->free();
		if ($this->ddSrc=="information_schema") $this->closeMysqli(1);
		if ($count==0) return false;
	  	$this->columns[$tableName]['rawCols']=$result;
		foreach ($result as $col) {
			$this->columns[$tableName]['iCols'][$col['COLUMN_NAME']]=$col;
			if ($col['COLUMN_KEY']=='PRI') $this->columns[$tableName]['pk']=$col['COLUMN_NAME']; // Cache the PK separately
		}
		return $this->columns[$tableName];
	}

	function tableExists($tableName) {
		if (!isset($this->columns[$tableName])) {
			$this->loadColumns($tableName,true);
		}
		return sizeOf($this->columns[$tableName]['rawCols']);
	}

	function columnExists($tableName,$colName) {
		return ($this->getColFromDD($tableName,$colName))?true:false;
	}

	function getColsFromDD($tableName) {
	  // Results may be already cached
		if (!isset($this->columns[$tableName])) $this->loadColumns($tableName);
		return $this->columns[$tableName]['iCols'];
	}

	function getKeyCol($tableName) {
		if (!isset($this->columns[$tableName])) $this->loadColumns($tableName);
		if (!$this->columns[$tableName]) return false;
		return $this->columns[$tableName]['pk'];
	}

	function getForeignKeys($tableName) {
		$keys=[];
		if (!isset($this->columns[$tableName])) $this->loadColumns($tableName);
		foreach ($this->columns[$tableName]['rawCols'] as $col) {
			$len=strlen($col['COLUMN_NAME']);
		  	if (substr($col['COLUMN_NAME'],$len-2,2)=="ID") {
		    	$foreignTable=$this->getTableForPK($col['COLUMN_NAME']);
		    	if ($foreignTable) $keys[$col['COLUMN_NAME']]=$foreignTable;
		  	}
		}
		return $keys;
	}

	function getTableForPK($keyCol) {
    	$singular=substr($keyCol,0,strlen($keyCol)-2);
    	if (!$this->plural) $this->plural = new Inflect();
    	$plural = $this->plural->pluralise($singular);
    	if ($this->tableExists($plural) && $this->columnExists($plural,$keyCol)) {
    		return $plural;    
    	}
    	return false; // Full DD hunt?
  	}

	// Return 1 column definition from the Data Dictionary
	function getColFromDD($tableName,$colName) {
		if (!isset($this->columns[$tableName])) $this->loadColumns($tableName);
		return (isset($this->columns[$tableName]['iCols'][$colName]))?$this->columns[$tableName]['iCols'][$colName]:false;
	}

}

?>