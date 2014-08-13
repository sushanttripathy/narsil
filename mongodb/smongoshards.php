<?php

include_once  __DIR__.'/smongoclient.php';
include_once  __DIR__.'/../clients/sclientsharding.php';

define('READ_TYPE_GET', 1);
define('READ_TYPE_GETONE', 2);
define('READ_TYPE_COUNT', 3);
define('WRITE_TYPE_SET', 4);
define('WRITE_TYPE_PUSH', 5);
define('WRITE_TYPE_INCLUSIVESET', 6);
define('WRITE_TYPE_STRICTSET', 7);
define('WRITE_TYPE_SIMPLESET', 8);
define('WRITE_TYPE_INCREMENT', 9);
define('WRITE_TYPE_INSERT', 10);
define('DELETE_TYPE_DELETE', 11);
define('DELETE_TYPE_REMOVE', 12);

class SMongoShards extends SClientSharding
{
	protected $db_name;
	protected $column_name;
	
	function __construct($servers = null, $db_name='default', $column_name='default', $replicas = 3, $assemble_type = ASSEMBLE_CONSENSUS)
	{
		$this->db_name = $db_name;
		$this->column_name = $column_name;
		
		$replicas = max(1,$replicas);
		
		if(empty($servers))
		{
			$servers = SMongoServers::GetServersInfo();
		}
		@parent::__construct($servers, $replicas, false, true, $assemble_type, 'ts_');//ASSEMBLE_CONSENSUS, 'ts_');
	}
	
	function ConnectToServer($server_index, $extra=null, $bogus = null)
	{
        $server_inf =  $this->servers_list[$server_index];
		$server_details = "mongodb://".$server_inf['host'].":".$server_inf['port'];
		
		$db = $this->db_name;
		
		$column = $this->column_name;
	
		if(!empty($server_details))
		{
			$connection_obj = new SMongoClient($server_details, $db, $column);
	
			@parent::ConnectToServer($server_index, null, $connection_obj);
			return true;
		}
		return false;
	}
	
	function DisconnectFromServer($server_index, $extra = null)
	{
		$connection_obj = $this->connections_list[$server_index];
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			$connection_obj->Close();
		}
		@parent::DisconnectFromServer($server_index, null);
	}
	
	function ReadData($server_index, $key, $condition_and_read_type)
	{
		$condition = $condition_and_read_type['condition'];
		$limit = $condition_and_read_type['limit'];
		$offset = $condition_and_read_type['offset'];
		$sort_by = $condition_and_read_type['sort_by'];
		
		$read_type = $condition_and_read_type['read_type'];
		$filter = (isset($condition_and_read_type['filter'])?$condition_and_read_type['filter']:null);
		
		$connection_obj = $this->connections_list[$server_index];
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			switch($read_type)
			{
				case READ_TYPE_COUNT:
					return $connection_obj->Count($condition);
					
				case READ_TYPE_GETONE:
					return $connection_obj->GetOne($condition, $filter);
					
				case READ_TYPE_GET:
				default:
					return $connection_obj->Get( $condition, $limit, $offset, $sort_by);
			}
		}
		return null;
	}
	
	function WriteData($server_index, $key, $data, $condition_and_write_type)
	{
		$condition = $condition_and_write_type['condition'];
		$write_type = $condition_and_write_type['write_type'];
		$multi =  $condition_and_write_type['multi'];
		
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			switch($write_type)
			{
				case WRITE_TYPE_INCLUSIVESET:
					return $connection_obj->InclusiveSet($condition, $data, $multi);
				case WRITE_TYPE_INCREMENT:
					return $connection_obj->Increment($condition, $data, $multi);
				case WRITE_TYPE_INSERT:
					return $connection_obj->Insert($data);
				case WRITE_TYPE_PUSH:
					return $connection_obj->Push($condition, $data, $multi);
				case WRITE_TYPE_SIMPLESET:
					return $connection_obj->SimpleSet($condition, $data, $multi);
				case WRITE_TYPE_STRICTSET:
					return $connection_obj->StrictSet($condition, $data, $multi);
				case WRITE_TYPE_SET:
				default:
					return $connection_obj->Set($condition, $data, $multi);
			}
		}
		return false;
	}
	
	function DeleteData($server_index, $key, $condition_and_delete_type)
	{
		$condition = $condition_and_delete_type['condition'];
		$delete_set =  $condition_and_delete_type['delete_set'];
		$delete_type = $condition_and_delete_type['delete_type'];
		
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			switch($delete_type)
			{
				case DELETE_TYPE_REMOVE:
				default:
					return $connection_obj->Remove($condition);
				case DELETE_TYPE_DELETE:
					return $connection_obj->Delete($condition, $delete_set);
			}
		}
		return false;
	}
	
	protected function CleanupDataBeforeAssembly($data_arr)
	{
		$cleaned_arr = array();
		
		if(!empty($data_arr) && is_array($data_arr))
		{
			foreach($data_arr as $key => $value)
			{
				foreach($value as $sub_key => $sub_value)
				{
					if($sub_key != '_id')
					{
						$cleaned_arr[$key][$sub_key] = $sub_value;
					}
				}
			}
		}
		else
		{
			$cleaned_arr = $data_arr;
		}
		
		//print_r($cleaned_arr);
		
		return $cleaned_arr;
	}
	
	protected function MakeGetArray($condition, $filter = null, $limit = 1000, $offset = 0, $sort_by = null, $get_type = READ_TYPE_GET)
	{
		return array('condition'=>$condition, 'filter'=>$filter, 'limit'=>$limit,'offset'=>$offset, 'sort_by'=>$sort_by,'read_type'=>$get_type);
	}
	
	function Count($condition = null, $key,  $tsCol = 'ts_')
	{
		$get_arr = $this->MakeGetArray($condition, null, null, null, null, READ_TYPE_COUNT);
		
		return @parent::Get($key, $get_arr, $tsCol );
	}
	
	function Get($condition, $key, $limit = 1000, $offset = 0, $sort_by = null,  $tsCol = 'ts_')//, $internalDataSave = false)
	{
		$get_arr = $this->MakeGetArray($condition, null, $limit, $offset, $sort_by, READ_TYPE_GET);
		$res = @parent::Get($key, $get_arr, $tsCol );
		
		if(!empty($res) && is_array($res))
		{
			$res2 = array();
			foreach($res as $key => $value)
			{
				//$res_index = $value['_id']->id;
				//print_r($res_index);
				//print_r($value);
				unset($value['_id']);
				$res2[$key] = $value;
			}
			return $res2;
		}
		return $res;
	}
	
	function GetOne($condition, $key, $filter = null, $tsCol = 'ts_')//, $internalDataSave = false)
	{
		$get_arr = $this->MakeGetArray($condition, $filter, 1000, 0, null, READ_TYPE_GETONE);
		$res = @parent::Get($key, $get_arr, $tsCol );
		
		if(!empty($res) && isset($res['_id']))
		{
			unset($res['_id']);
		}
		
		//print_r($res);
		
		return $res;
	}
	
	protected function MakeSetArray($condition,  $write_type = WRITE_TYPE_SET, $multi = false)
	{
		return array('condition'=>$condition, 'write_type'=>$write_type, 'multi'=>$multi);
	}
	
	function Set($condition, $data, $key, $multi = false, $tsCol = 'ts_', $uniqid = '__id')
	{
		if(empty($condition) && empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray($condition, WRITE_TYPE_SET, $multi);
		
		if(is_array($data))
		{
			$data = array_merge($data, array($tsCol => time(), $uniqid => uniqid()));
		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[$uniqid] = uniqid();
			$temp[] = $data;
				
			$data = $temp;
		}
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function Push($condition, $data, $key, $multi = false, $tsCol = 'ts_', $uniqid = '__id')
	{
		if(empty($condition) && empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray($condition, WRITE_TYPE_PUSH, $multi);
		
		if(is_array($data))
		{
			$data = array_merge($data, array($tsCol => time(), $uniqid => uniqid()));
		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[$uniqid] = uniqid();
			$temp[] = $data;
				
			$data = $temp;
		}
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function InclusiveSet($condition, $data, $key, $multi = false, $tsCol = 'ts_', $uniqid = '__id')
	{
		if(empty($condition) && empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray($condition, WRITE_TYPE_INCLUSIVESET, $multi);
		
		if(is_array($data))
		{
			$data = array_merge($data, array($tsCol => time(), $uniqid => uniqid()));
		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[$uniqid] = uniqid();
			$temp[] = $data;
				
			$data = $temp;
		}
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function StrictSet($condition, $data, $key, $multi = false, $tsCol = 'ts_', $uniqid = '__id')
	{
		if(empty($condition) && empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray($condition, WRITE_TYPE_STRICTSET, $multi);
		
		if(is_array($data))
		{
			$data = array_merge($data, array($tsCol => time(), $uniqid => uniqid()));
		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[$uniqid] = uniqid();
			$temp[] = $data;
				
			$data = $temp;
		}
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function SimpleSet($condition, $data, $key, $multi = false, $tsCol = 'ts_', $uniqid = '__id')
	{
		if(empty($condition) && empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray($condition, WRITE_TYPE_SIMPLESET, $multi);
		
		if(is_array($data))
		{
			$data = array_merge($data, array($tsCol => time(), $uniqid => uniqid()));
		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[$uniqid] = uniqid();
			$temp[] = $data;
				
			$data = $temp;
		}
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function Increment($condition, $data,  $key, $multi = false)
	{
		if(empty($condition) && empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray($condition, WRITE_TYPE_INCREMENT, $multi);
		
		//$data = $data + 0;
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function Insert($data, $key, $tsCol = 'ts_', $uniqid = "__id")
	{
		if(empty($data))
		{
			return null;
		}
		
		$set_arr = $this->MakeSetArray(null, WRITE_TYPE_INSERT);
		
		if(is_array($data))
		{
			$data = array_merge($data, array($tsCol => time(), $uniqid=>uniqid()));//$uniqid => uniqid()));

		}
		else
		{
			$temp = array();
			$temp[$tsCol] = time();
			$temp[$uniqid] = uniqid();
			$temp[] = $data;
			$data = $temp;
		}
		
		return @parent::Set($key, $data, $set_arr);
	}
	
	function MakeDelArray($condition, $delete_set = null, $delete_type = DELETE_TYPE_REMOVE)
	{
		return array('condition'=>$condition, 'delete_set'=>$delete_set, 'delete_type'=>$delete_type);
	}
	
	function Delete($condition,$delSet, $key)
	{
		if(!empty($condition) && !empty($delSet))
		{
			$del_arr = $this->MakeDelArray($condition, $delSet, DELETE_TYPE_DELETE);
			
			return @parent::Del($key, $del_arr);
		}
		return null;
	}
	
	function Remove($condition, $key)
	{
		if(!empty($condition))
		{
			$del_arr = $this->MakeDelArray($condition, null, DELETE_TYPE_REMOVE);
				
			return @parent::Del($key, $del_arr);
		}
		return null;
	}
}