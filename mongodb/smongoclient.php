<?php

include_once  __DIR__.'/mongodb_servers.php';
include_once  __DIR__.'/../clients/sclientskeleton.php';

class SMongoClient extends SClientSkeleton
{
	protected $db_name;
	protected $column_name;
	
	protected $db;
	protected $column;
	
	function __construct($host=null, $db=null, $column=null)
	{
		if(empty($host))
		{
            $servers = SMongoServers::GetServersInfo();
			$count = count ( $servers );
				
			$index = rand ()%$count;

            $server_inf = $servers[$index];
			
			$host =  "mongodb://".$server_inf['host'].":".$server_inf['port'];
		}
		
		if(empty($db))
		{
			$db = 'default';
		}
		
		if(empty($column))
		{
			$column = 'default';
		}
		
		$this->db_name = $db;
		
		$this->column_name = $column;
		
		@parent::__construct(array($host), null, true);
	}
	
	function ConnectToServer()
	{
		$host = $this->server_details[0];
		
		$mongo_obj = $this->GetExistingConnectionFromPool($this->server_details);
		
		if(empty($mongo_obj) || !is_object($mongo_obj))
		{
			$mongo_obj = new Mongo($host);
		}
		return $mongo_obj;
	}
	
	function IsConnected($mongo_obj)
	{
		$test_db = $mongo_obj->test;
		
		if(!empty($test_db) && is_object($test_db))
		{
			return $test_db->command(array('ping'=>1));
		}
		return false;
	}
	
	function ConnectToDBAndColumn()
	{
		$mongo_obj = $this->GetOrMakeServerConnection();
		
		if(!empty($mongo_obj) && is_object($mongo_obj))
		{
			$db_name = $this->db_name;
			$this->db = $mongo_obj->$db_name;
			
			if(!empty($this->db) && is_object($this->db))
			{
				$column_name  = $this->column_name;
				$this->column = $this->db->$column_name;
				
				if(!empty($this->column) && is_object($this->column))
				{
					return true;
				}
			}
		}
		return false;
	}
	
	function o_Get($condtion, $limit = 1000, $offset = 0, $sort_array = null, $ensure_index = true)
	{
		if($this->ConnectToDBAndColumn())
		{
			$cursor = $this->column->find($condtion);
			if(!empty($cursor) && is_object($cursor))
			{
				if(!empty($offset))
				{
					$cursor->skip($offset+0);
				}
				
				$cursor->limit($limit);
				
				if(!empty($sort_array) && is_array($sort_array))
				{
					if($ensure_index)
					{
						$this->column->ensureIndex($sort_array);
					}
					
					$cursor->sort($sort_array);
				}
				
				return iterator_to_array($cursor);
			}
		}
		return null;
	}
	
	function o_GetOne($condition, $filter = null)
	{
		if($this->ConnectToDBAndColumn())
		{
			if(empty($filter))
				return $this->column->findOne($condition);
			else
				return $this->column->findOne($condition,$filter);
		}
		return null;
	}
	
	function o_Set($condition, $set)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, array('$set'=>$set), array("upsert" => true, "safe"=>true));
		}
		return false;
	}
	
// 	function o_Update($condition, $set)
// 	{
// 		if($this->ConnectToDBAndColumn())
// 		{
// 			return $this->column->update($condition, array('$set'=>$set), array("upsert" => true, "safe"=>true));
// 		}
// 		return false;
// 	}
	
	function o_Push($condition, $set, $multi=false)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, array('$push'=>$set), array("upsert" => true,"safe"=>true, "multiple"=>$multi));
		}
		return false;
	}
	
	function o_InclusiveSet($condition, $set, $multi=false)//$condition is an array(), $set is an array()
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, array('$addToSet'=>$set), array("upsert" => true,"safe"=>true, "multiple"=>$multi));
		}
		return false;
	}
	
	function o_StrictSet($condition, $set, $multi=false)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, array('$set'=>$set), array("upsert" => true, "safe"=>true, "multiple"=>$multi));
		}
		return false;
	}
	
	function o_SimpleSet($condition, $set, $multi=false)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, $set, array("upsert" => true, "safe"=>true, "multiple"=>$multi));//unsafe
		}
		return false;
	}
	
	function o_Increment($condition, $set, $multi=false)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, array('$inc'=>$set), array("upsert" => true, "safe"=>true, "multiple"=>$multi));//unsafe
		}
		return false;
	}
	
	function o_Insert($set)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->insert($set);
		}
		return false;
	}
	
	function o_Count($condition = null)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->count($condition);
		}
		return false;
	}
	
	function o_Delete($condition, $delSet)//$condition is an array(), $delSet is a string
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->update($condition, array('$unset'=>array($delSet=>1)));
		}
		return false;
	}
	
	function o_Remove($condition)
	{
		if($this->ConnectToDBAndColumn())
		{
			return $this->column->remove($condition);
		}
		return false;
	}
	
	function o_GetLastError()
	{
		if($this->db)
		{
			return $this->db->lastError();
		}
		return '';
	}
	
	function o_Close()
	{
		
	}
}