<?php

include_once  __DIR__.'/mysql_servers.php';
include_once __DIR__.'/../generateid/sgenerateid.php';

include_once  __DIR__.'/../clients/sclientsharding.php';
include_once  __DIR__.'/smysqli.php';

define('__2_ENABLE_DEBUG', false);


class SMySQLiShards extends SClientSharding
{
	protected $tablename = null;
	
	function __construct($servers = null, $tablename = 'user_auth', $replicas = 3)
	{
		$this->tablename = $tablename;
		$replicas = max(1,$replicas);
		
		if(empty($servers))
		{
			$servers = SMySQLiServers::GetServersInfo();
		}
		
		@parent::__construct($servers, $replicas, false, true, ASSEMBLE_CONSENSUS);
	}
	
	function ConnectToServer($server_index, $extra=null, $bogus = null)
	{
		$server_details = $this->servers_list[$server_index];
		
		if(!empty($server_details))
		{
			$connection_obj = new SMySQLiv2($this->servers_list[$server_index]);
				
			@parent::ConnectToServer($server_index, $extra, $connection_obj);
			return true;
		}
		return false;
	}
	
	function DisconnectFromServer($server_index, $extra = NULL)
	{
		$connection_obj = $this->connections_list[$server_index];
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			$connection_obj->close();
		}
		@parent::DisconnectFromServer($server_index, $extra);
	}
	
	function ReadData($server_index, $key, $condition)
	{
		$connection_obj = $this->connections_list[$server_index];
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->Get($this->tablename, $condition);
		}
		return null;
	}
	
	function WriteData($server_index, $key, $data, $condition)
	{
		$connection_obj = $this->connections_list[$server_index];
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->Set($this->tablename, $condition, $data);
		}
		return false;
	}
	
	function DeleteData($server_index, $key, $condition)
	{
		$connection_obj = $this->connections_list[$server_index];
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->Del($this->tablename, $condition);
		}
		return false;
	}
	
	function SetTable($tablename)
	{
		$this->tablename = $tablename;
	}
	
	/**
	 * @param array $condition The array of conditions, for example : array('col1'=>11, 'col2' => 12), all conditions are joined by an AND clause
	 * @param string $key The key string based on which data is partitioned
	 * @param string $tsCol The timestamp column, defaults to 'ts'
	 * @return multitype: All the rows that match $condition
	 * @desc Returns all the rows that match a certain condtion, unlike Get which returns only one result row
	 */
	function MultiGet($condition, $key, $tsCol = 'ts')
	{
		$query = SMySQLiv2::QueryBuilder(MYSQL_QUERY_SELECT, $this->tablename, $condition);
		
		$servers_arr = array();
		$this->PrepareServerGroupForKey($key, null, $servers_arr);
		
		$result_rows = array();
		$fin_res = array();
		
		if(!empty($servers_arr))
		{
			foreach($servers_arr as $server_index)
			{
				$connection_obj = $this->connections_list[$server_index];
				
				if(($result = $connection_obj->query($query)))
				{
					$result_rows[] = $result->fetch_all(MYSQLI_ASSOC);
					$result->free();
				}
				else
				{
					$result_rows[] = null;
				}
			}
			
			if(!empty($result_rows))
			{
				$res_rows_count = count($result_rows);
				
				$exists = array();
				$notexists = array();
				
				$latest = array(); //this array will store the timestamps
				
				for($i = 0; $i < $res_rows_count; $i++)
				{
					foreach($result_rows[$i] as $key_ => $value_)
					{
						if(!isset($exists[$key_]))
						{
							$exists[$key_] = 0;
						}
						
						if(!isset($notexists[$key_]))
						{
							$notexists[$key_] = 0;
						}
						
						if(!isset($latest[$key_]))
						{
							$latest[$key_] = 0;
						}
						
						if(!empty($value_))
						{
							$exists[$key_]++;
						}
						else
						{
							$notexists[$key_]++;
						}
						
						if(!empty($value_[$tsCol]) && $value_[$tsCol]+0 > $latest[$key_])
						{
							$fin_res[$key_] = $value_;
							$latest[$key_] = $value_[$tsCol]+0;
						}
					}
				}
				
				if(!empty($fin_res))
				{
					foreach($fin_res as $key_ => $value_)
					{
						if($notexists[$key_] > $exists[$key_])
						{
							unset($fin_res[$key_]);
						}
					}
				}
			}
		}
		
		return $fin_res;
	}
	
	function Get($condition, $key, $tsCol = 'ts')//, $internalDataSave = false)
	{
		$res = @parent::Get($key, $condition, $tsCol );
		return $res;
	}
	
	function Set($condition, $set, $key, $tsCol = 'ts', $primaryindex = 'id', $suppliedPrimaryIndex = null)
	{
		if(empty($condition) && empty($set))
		{
			return null;
		}
		
		if(is_array($set))
		{
			$set = array_merge($set, array($tsCol => time()));
		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[] = $set;
			
			$set = $temp;
		}
		
		if(!empty($condition))
			$res = $this->Get($condition, $key, $tsCol);
		else
			$res = null;
		
		if(!empty($res))
		{
			$primIndexVal = $res[$primaryindex];
			
			if(!empty($condition) && is_array($condition))
				$condition = array_merge( $condition, array($primaryindex=>$primIndexVal));
			else
				$condition = array($primaryindex=>$primIndexVal);
		}
		else if(empty($suppliedPrimaryIndex))
		{
			$SgenerateID = new SGenerateID();
			$primIndexVal = $SgenerateID->getNewID();
			
			if(!empty($condition) && is_array($condition))
				$condition = array_merge( $condition, array($primaryindex=>$primIndexVal));
			else
				$condition = array($primaryindex=>$primIndexVal);
			
			$set = array_merge($set, $condition);
		}
		else
		{
			$primIndexVal = $suppliedPrimaryIndex;
			
			if(!empty($condition) && is_array($condition))
				$condition = array_merge( $condition, array($primaryindex=>$primIndexVal));
			else
				$condition = array($primaryindex=>$primIndexVal);
			
			$set = array_merge($set, $condition);
		}
		
		$success = @parent::Set($key, $set, $condition);
		
		return $success?$primIndexVal:null;
	}
	
	function SetById($id, $set, $key, $tsCol = 'ts', $primaryindex = 'id', $usesuppliedID = false)
	{
		if(empty($id) && empty($set))
		{
			return null;
		}
		
		$condition = array($primaryindex => $id);
		
		$res = $this->Get($condition, $key, $tsCol, true);
		
		if(!empty($res))
		{
			$primIndexVal = $res[$primaryindex];
			
		}
		else if($usesuppliedID)
		{
			$primIndexVal = $id;
			
			$condition = null;
			$set = array_merge($set, array($primaryindex=>$primIndexVal));
		}
		else
		{
			$SgenerateID = new SGenerateID();
			$primIndexVal = $SgenerateID->getNewID();
			
			$condition = null;
			$set = array_merge($set, array($primaryindex=>$primIndexVal));
		}
		
		$set =  array_merge($set, array($tsCol => time()));
		
		$success = @parent::Set($key, $set, $condition);
		
		return $success?$primIndexVal:null;
	}
	
	function Delete($condition, $primaryindex = 'id',$key)
	{
		if(empty($condition))
		{
			return null;
		}
		
		return @parent::Del($key, $condition);
	}
	
	function DeleteById($id, $primaryindex = 'id', $key)
	{
		if(empty($id))
		{
			return null;
		}
		
		$condition = array($primaryindex => $id);
		
		return @parent::Del($key, $condition);
	}
}
?>