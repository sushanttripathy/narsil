<?php

define('MYSQL_QUERY_SELECT', 0);
define('MYSQL_QUERY_INSERT', 1);
define('MYSQL_QUERY_UPDATE', 2);
define('MYSQL_QUERY_DELETE', 3);

define('ENABLE_FAULT_TOLERANCE', true);

include_once   __DIR__.'/mysql_servers.php';

include_once  __DIR__.'/../clients/sclientskeleton.php';

/**
 * MySQLi Connect Exception class
 *
 * This exception will be thrown, when we are unable to connect to MySQL Server.
 */
class SMySQLiConnectException extends Exception {
	
	public function __construct($error, $errno = 0) {
		parent::__construct ( $error, $errno );
	}
}

/**
 * DB Query Exception class
 *
 * This exception will be thrown, when MySQL server is unable to process our SQL.
 */
class SMySQLiQueryException extends Exception {
	
	public function __construct($error, $errno = 0) {
		parent::__construct ( $error, $errno );
	}
}

/**
 * Our base DB class, that extends mysqli.
 *
 */
class SMySQLi extends mysqli {
	
	protected $server_index = null;
	
	/**
	 * We'll overwrite parent __construct, so we can attach our DBConnectException 
	 * exception upon any failure.
	 *
	 * We'll try to provide all the arguments parent class provides.
	 * @param string $host MySQL hostname
	 * @param string $user MySQL username
	 * @param string $pass MySQL password (use null for no password)
	 * @param string $db MySQL database to select (use null for none)
	 * @param string $port MySQL port to connect to (use null for default)
	 * @param string $socket MySQL socket to be used (use null for default)
	 * @throws DBConnectException
	 */
	public function __construct(/*$host = null, $user = null, $pass = null, $db = null, $port = null, $socket = null,*/ $servers = null) {
		
		
		if (empty ( $servers )) {
            $servers = SMySQLiServers::GetServersInfo();
			if (empty ( $servers )) {
				// execute parent constructor that will try to connect, 
				// use @ operator, to supress any error output
				@parent::__construct ();
			} else {
				$count = count ( $servers );
				
				$index = rand ( 0, $count-1 );
				
				$host = $servers [$index] ['host'];
				$port = $servers [$index] ['port'];
				$user = $servers [$index] ['user'];
				$pass = $servers [$index] ['pass'];
				
				
				$db = 	$servers ['db'];
				$socket = null;
				// execute parent constructor that will try to connect, 
				// use @ operator, to supress any error output
				

				@parent::__construct ( $host, $user, $pass, $db, $port, $socket );
				
				if(ENABLE_FAULT_TOLERANCE)
				{
					if ($this->connect_errno != 0)
					{
						for($i = $index+1; $i < $index + TOTAL_MYSQL_SERVERS; $i++)
						{
							$host = $servers [($i%TOTAL_MYSQL_SERVERS)] ['host'];
							$port = $servers [($i%TOTAL_MYSQL_SERVERS)] ['port'];
							$user = $servers [($i%TOTAL_MYSQL_SERVERS)] ['user'];
							$pass = $servers [($i%TOTAL_MYSQL_SERVERS)] ['pass'];
							$db = 	$servers [($i%TOTAL_MYSQL_SERVERS)] ['db'];
							
							@parent::__construct ( $host, $user, $pass, $db, $port, $socket );
							
							if($this->connect_errno == 0)
							{
								//connection successful
								break;
							}
						}
					}
				}
			}
		}
		
		// check if connect errno is set
		if ($this->connect_errno != 0) {
			// error has occoured, throw our DBConnectException with 
			// error message and error code
			throw new SMySQLiConnectException ( $this->connect_error, $this->connect_errno );
		}
		else 
		{
			if(isset($index))
			{
				$this->server_index = $index;
			}
		}
	}
	
	function GetServerIndex()
	{
		return $this->server_index;
	}

	/*
	 * After scaling out the connection  method I do not need to override the query method
	 */
	
/**
 * Query method
 *
 * @param string $sql SQL to execute
 * @return mysqli_result Object
 * @throws DBQueryException 
 */
/*
  public function query($sql){
    // here, we will log the query to sql.log file
    // note that, no error check is being made for this file
    file_put_contents('/tmp/sql.log', $sql . "\n", FILE_APPEND);
    // on with query execution, call the parent query method
    // call it with @ operator, to supress error messages
    $result = @parent::query($sql);
    // check if errno is set
    if ($this->errno != 0){
      // throw our DBQueryException with error message and error code
      throw new SMySQLiQueryException($this->error, $this->errno);
    }
    // if everything is OK, return the mysqli_result object
    // that is returned from parent query method
    return $result;
  }
  */

}

/**
 * @author Sushant
 * @desc SMySQLiv2 class handles mysqli object interface for connection to a MySQL database
 */
class SMySQLiv2 extends SClientSkeleton
{
	protected $server_index = null;
	protected $self_assigned_server_index = false;
	
	/**
	 * @param mixed $host Is usually the fully qualified server domain name or ip, if it is an array, SMySQLiv2 will look for these keys 'host', 'port', 'user', 'pass', 'db'
	 * @param string $username
	 * @param string $passwd
	 * @param string $db
	 * @param integer $port
	 * @return nothing
	 */
	function __construct($host=null, $user = null, $pass = null, $db = null, $port = null)
	{
		if (!empty($host) && is_array($host))
		{
			$port = $host ['port'];
			$user = $host ['user'];
			$pass = $host ['pass'];
		
		
			$db = $host ['db'];
				
			$host = $host ['host'];
				
			$this->self_assigned_server_index = false;
		}
		else if(empty($host) || empty($user) || empty($pass) || empty($db) || empty($port))
		{
            $servers_info = SMySQLiServers::GetServersInfo();
			$count = count ( $servers_info );
				
			$index = rand ( 0, $count-1 );
			
			$host = $servers_info [$index] ['host'];
			$port = $servers_info [$index] ['port'];
			$user = $servers_info [$index] ['user'];
			$pass = $servers_info [$index] ['pass'];
			
			
			$db = 	$servers_info [$index] ['db'];
			
			$this->server_index = $index;
			$this->self_assigned_server_index = true;
		}
		else
		{
			$this->self_assigned_server_index = false;
		}
		
		@parent::__construct(array($host, $user, $pass, $db, $port), null, true);
	}
	
	function GetServerIndex()
	{
		return $this->server_index;
	}
	
	function ConnectToServer()
	{
		$host = $this->server_details[0];
		$user = $this->server_details[1];
		$pass = $this->server_details[2];
		$db = $this->server_details[3];
		$port = $this->server_details[4];
		
		$server_details = array($host, $user, $pass, $db, $port);
			
		$MySQLiObj = $this->GetExistingConnectionFromPool($server_details);
			
		if(empty($MySQLiObj))
		{
			$MySQLiObj = new mysqli($host, $user, $pass, $db, $port);
		}
		
		$index = $this->server_index + 0;
		
		if(ENABLE_FAULT_TOLERANCE && $this->self_assigned_server_index)
		{
			if (!is_object($MySQLiObj) || $MySQLiObj->connect_errno != 0)
			{
				if(is_object($MySQLiObj))
					$MySQLiObj->close();

                $servers_info = SMySQLiServers::GetServersInfo();

				for($i = $index+1; $i < $index + TOTAL_MYSQL_SERVERS; $i++)
				{
					$host = $servers_info [($i%TOTAL_MYSQL_SERVERS)] ['host'];
					$port = $servers_info [($i%TOTAL_MYSQL_SERVERS)] ['port'];
					$user = $servers_info [($i%TOTAL_MYSQL_SERVERS)] ['user'];
					$pass = $servers_info [($i%TOTAL_MYSQL_SERVERS)] ['pass'];
					$db = 	$servers_info [($i%TOTAL_MYSQL_SERVERS)] ['db'];

					$server_details = array($host, $user, $pass, $db, $port);
					
					$MySQLiObj = $this->GetExistingConnectionFromPool($server_details);
					
					if(empty($MySQLiObj))
						$MySQLiObj = new mysqli ( $host, $user, $pass, $db, $port );
						
					if($MySQLiObj->connect_errno == 0)
					{
						//connection successful
						$this->server_index = ($i%TOTAL_MYSQL_SERVERS);
						break;
					}
				}
				@parent::SetServerDetails($server_details);
			}
		}
		
		return $MySQLiObj;
	}
	
	function IsConnected($MySQLiObj)
	{
		$res = $MySQLiObj->ping();
		return $res;
	}
	
	function CleanupLostConnection($MySQLiObj)
	{
		return $MySQLiObj->close();
	}
	
	function o_Get($table, $condition)
	{
		$query = $this->QueryBuilder(MYSQL_QUERY_SELECT, $table, $condition, null);
		
		$server_obj_arr = array();
		
		$MySQLiObj = $this->GetOrMakeServerConnection();
		
		
		if(($result = $MySQLiObj->query($query)))
		{
			$row = $result->fetch_assoc();//fetch_all(MYSQLI_ASSOC);
			$result->free();
		}
		else
		{
			return $row = null;
		}
		return $row;
	}
	
	function o_Set($table, $condition, $set)
	{
		//Decide what it is going to be, an insert or an update?
		if(!empty($condition))
			$results = $this->Get($table, $condition);
		else 
			$results = null;
		
		if(!empty($results))
		{
			//Update
			$query = $this->QueryBuilder(MYSQL_QUERY_UPDATE, $table, $condition, $set);
		}
		else
		{
			//Insert
			$query = $this->QueryBuilder(MYSQL_QUERY_INSERT, $table, $condition, $set);
		}
		
		$MySQLiObj = $this->GetOrMakeServerConnection();
		
		return $MySQLiObj->query($query);
	}
	
	function o_Del($table, $condition)
	{
		$query = $this->QueryBuilder(MYSQL_QUERY_DELETE, $table, $condition);
		$MySQLiObj = $this->GetOrMakeServerConnection();
		return $MySQLiObj->query($query);
	}
	
	static function QueryBuilder($query_type = MYSQL_QUERY_SELECT, $table, $condition =null, $set = null)
	{
		$query = '';
		
		switch($query_type)
		{
			case MYSQL_QUERY_SELECT:
				{
					$select_what = '*';
					
					if(!empty($set))
					{
						if(is_array($set))
						{
							$select_what = implode('`,`', $set);
							$select_what = '`'.$select_what.'`';
						}
						else
						{
							$select_what = '`'. $set.'`';
						}
					}
					
					$where = '';
					
					$where_flag = false;
					
					if(!empty($condition))
					{
						if(is_array($condition))
						{
							foreach($condition as $key => $value)
							{
								if($where_flag)
								{
									$where .= ' AND ';
								}
								$where .= "`$key` = '$value'";
								$where_flag = true;
							}
						}
						else
						{
							$where = $condition;
						}
					}
					else
					{
						$where = '0';
					}
					
					if(!empty($table))
					{
						$table = '`'.$table.'`';
					}
					else
					{
						return null;
					}
					
					$query = " SELECT $select_what FROM $table WHERE $where";
					
				}
				break;
				
			case MYSQL_QUERY_INSERT:
				{
					if(!empty($table))
					{
						$table = '`'.$table.'`';
					}
					else
					{
						return null;
					}
					
					/*
					if(!empty($condition))
					{
						if(is_array($condition))
						{
							$columns = implode('`,`', $condition);
							$columns = '(`'. $columns.'`)';
						}
						else
						{
							$columns = '('. $condition.')';
						}
					}
					else
					{
						return null;
					}*/
					
					$columns_arr = array();
					$values_arr = array();
					
					if(!empty($set) && is_array($set))
					{
						foreach($set as $key => $value)
						{
							if(is_string($key))
							{
								$columns_arr[] = '`'.$key.'`';
							}
							$values_arr[] = '\''.$value.'\'';
						}
					}
					else
					{
						return null;
					}
					
					if(!empty($columns_arr))
					{
						$columns = '('.implode(',', $columns_arr).')';
					}
					else
					{
						$columns = '';
					}
					
					$values = '('.implode(',', $values_arr).')';
					
					$query = "INSERT INTO $table$columns VALUES$values";
				}
				break;
				
			case MYSQL_QUERY_UPDATE:
				{
					if(!empty($table))
					{
						$table = '`'.$table.'`';
					}
					else
					{
						return null;
					}
					
					$update = '';
					$update_flag = false;
					
					if(!empty($set))
					{
						if(is_array($set))
						{
							foreach($set as $key => $value)
							{
								if($update_flag)
								{
									$update .= ',';
								}
								$update .= "`$key` = '$value'";
								$update_flag = true;
							}
						}
						else
						{
							$update = $set;
						}
					}
					else
					{
						return null;
					}
					
					$where = '';
					$where_flag = false;
					
					if(!empty($condition))
					{
						if(is_array($condition))
						{
							foreach($condition as $key => $value)
							{
								if($where_flag)
								{
									$where .= ' AND ';
								}
								$where .= "`$key` = '$value'";
								$where_flag = true;
							}
						}
						else
						{
							$where = $condition;
						}
					}
					else
					{
						$where = '0';
					}
					
					$query = "UPDATE $table SET $update WHERE $where";
				}
				break;
				
			case MYSQL_QUERY_DELETE:
				{
					if(!empty($table))
					{
						$table = '`'.$table.'`';
					}
					else
					{
						return null;
					}
					
					$where = '';
					$where_flag = false;
					
					if(!empty($condition))
					{
						if(is_array($condition))
						{
							foreach($condition as $key => $value)
							{
								if($where_flag)
								{
									$where .= ' AND ';
								}
								$where .= "`$key` = '$value'";
								$where_flag = true;
							}
						}
						else
						{
							$where = $condition;
						}
					}
					else
					{
						$where = '0';
					}
					
					$query = "DELETE FROM $table WHERE $where";
				}
				break;
		}
		
		return $query;
	}
}
?>