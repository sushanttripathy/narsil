<?php

/*
 * Override the following funcitons
 * 
 * ReadData
 * WriteData
 * DeleteData
 * ConnectToServer
 * DisconnectFromServer
 */


define ('ASSEMBLE_INCLUSIVE', 1);
define ('ASSEMBLE_LATEST', 2);
define ('ASSEMBLE_CONSENSUS', 3);


/**
 * @author Sushant
 *
 */
class SClientSharding 
{
	protected $servers_list = array();
	protected $connected_servers_list = array();
	protected $connections_list = array();
	protected $replicates = 1;
	protected $server_count = 0;
	protected $non_replicate_server_disconnect = true;
	protected $sync_servers = true;
	protected $assemble_data_method = ASSEMBLE_INCLUSIVE;
	protected $ts_col = 'ts';
	protected $extra = null;
	
	function __construct($servers_list, $replicates = 3, $non_replicate_server_disconnect = true, $sync_servers = true, $assemble_data_method = ASSEMBLE_INCLUSIVE, $ts_col = 'ts', $extra = null)
	{
		$this->servers_list = $servers_list;
		$this->replicates = $replicates;
		$this->server_count = (!empty($this->servers_list)&&is_array($this->servers_list))?count($this->servers_list):0;
		$this->non_replicate_server_disconnect = $non_replicate_server_disconnect;
		$this->sync_servers = $sync_servers;
		
		$this->assemble_data_method = $assemble_data_method;
		$this->ts_col = $ts_col;
		$this->extra = $extra;
	}
	
	protected function ValueMap($character)
	{
		$val = ord($character);
		
		if($val >= 48 && $val <= 57)
		{
			return $val - 48;
		}
		else if($val >= 65 && $val <= 90)
		{
			return 10 + ($val - 65);
		}
		else if($val >= 97 && $val <= 122)
		{
			return 10 + ($val - 97);
		}
		return 0;
	}
	
	function GetServerGroupForKey($key, $extra = null)
	{
		if(empty($this->server_count))
			return null;
		
		$real_key = md5($key);
		//Now figure out what length of the $real_key var do we extract to get our seed
		$length = floor(log($this->server_count)/log(36))+1;
		
		$seed_value = 0;
		
		for($i = 0; $i < $length; $i++)
		{
			$seed_value += $this->ValueMap($real_key{$i})*pow(36, $i);
		}
		
		$starting_index = $seed_value % $this->server_count;
		
		$ret = array();
		
		for($i = 0; $i < $this->replicates && $i < $this->server_count; $i++)
		{
			$ret[] = ($i + $starting_index)%$this->server_count;
		}
		return $ret;
	}
	
	/**
	 * @param string $key The key based on which server connections are partitioned
	 * @param mixed $extra Extra parameters that are passed to DisconnectFromServer, CheckIfConnectedToServer, ConnectToServer methods
	 * @param array $server_group_ext This function fills this pointer with the array of server indices that the current $key is supposed to be replicated amongst
	 * @return boolean Returns true on successfully obtaining a pool of connections for the $key
	 * @desc Connects to/checks existing connections to servers for the current $key. Please override DisconnectFromServer, CheckIfConnectedToServer, ConnectToServer methods 
	 * prior to invoking this method
	 */
	function PrepareServerGroupForKey($key, $extra = null, &$server_group_ext = null)
	{
		$server_group = $this->GetServerGroupForKey($key);
		
		if(empty($server_group))
			return false;
		
		if($this->non_replicate_server_disconnect)
		{
			foreach($this->connected_servers_list as $connected_server_index)
			{
				if(!in_array($connected_server_index, $server_group) && $this->CheckIfConnectedToServer($connected_server_index, $extra))
				{
					$this->DisconnectFromServer($connected_server_index, $extra);
				}
			}
		}
		
		foreach($server_group as $server_index)
		{
			if(!$this->CheckIfConnectedToServer($server_index, $extra))
			{
				$this->ConnectToServer($server_index, $extra);
			}
		}
		
		if($server_group_ext !== null)
		{
			$server_group_ext = $server_group;
		}
		
		return true;
	}
	
	function ReadData($server_index, $key, $extra = null)
	{
		//Override this function
		return null;
	}
	
	function WriteData($server_index, $key, $data, $extra = null)
	{
		//Override this function
		return false;
	}
	
	function DeleteData($server_index, $key, $extra = null)
	{
		//Override this function
		return false;
	}
	
	function GetServerIndex($server_details)
	{
		
	}
	
	function CheckIfConnectedToServer($server_index, $extra = null)
	{
		//Can be overridden for more specific server connection check
		if(in_array($server_index, $this->connected_servers_list))
		{
			return true;
		}
		return false;
	}
	
	function ConnectToServer($server_index, $extra = null, $established_connection = null)
	{
		//Override this function to connect to mofo server
		//this adds $established_connection to the connection pool
		
		if(!empty($established_connection))
		{
			$this->connected_servers_list[$server_index+0] = $server_index;
			$this->connections_list[$server_index+0] = $established_connection;
			return true;
		}
		return false;
	}
	
	function DisconnectFromServer($server_index, $extra = null)
	{
		//Override this function to disconnect from mofo server
		//this deletes the corresponding connection from connection pool array
		if(isset($this->connected_servers_list[$server_index+0]))
		{
			//array_splice($this->connected_servers_list, $server_index+0, 1);
			//array_splice($this->connections_list, $server_index+0, 1);
			unset($this->connected_servers_list[$server_index+0]);
			if(isset($this->connections_list[$server_index+0]))
			{
				unset($this->connections_list[$server_index+0]);
			}
			
			return true;
		}
		return false;
	}
	
	protected function CleanupDataBeforeAssembly($data_arr)
	{
		return $data_arr;
	}
	
	function AssembleData($data_arr, $method = ASSEMBLE_INCLUSIVE, $time_stamp_index = 'ts', $extra = null)
	{
		//returns assembled data
		//$data_arr is an array in the format array(0 => data_set_1, 1 => $data_set_2....)
		
		if(empty($data_arr) || !is_array($data_arr))
			return null;
		
		$fin_data = array();
		
		$data_arr = self::CleanupDataBeforeAssembly($data_arr);
		
		switch($method)
		{
			case ASSEMBLE_INCLUSIVE:
				$data_hash_array = array();
				foreach($data_arr as $data_key => $data)
				{
					$data_hash = md5(serialize($data));
					
					if(!in_array($data_hash, $data_hash_array))
					{
						if(!empty($data) && is_array($data))
						{
							$fin_data = array_merge($fin_data, $data);
						}
						else if(!empty($data))
						{
							$fin_data[] = $data;
						}
					}
					
					$data_hash_array[$data_key] = $data_hash;
				}
				
				break;
				
			case ASSEMBLE_LATEST:
				foreach($data_arr as $data)
				{
					if(empty($fin_data))
					{
						$fin_data = $data;
					}
					
					if($fin_data[$time_stamp_index] < $data[$time_stamp_index])
					{
						$fin_data = $data;
					}
				}
				break;
				
			case ASSEMBLE_CONSENSUS:
				$consensus_arr = array();
				$consensus_key_count = array();
				$consensus_key_map = array();
				
				foreach($data_arr as $arr_key => $data)
				{
					$consensus_key = md5(serialize($data));
					$consensus_arr[$arr_key] = $consensus_key;
					if(!isset($consensus_key_count[$consensus_key]))
					{
						$consensus_key_count[$consensus_key] = 0;
					}
					$consensus_key_count[$consensus_key]++;
					
					if(empty($consensus_key_map[$consensus_key]))
					{
						$consensus_key_map[$consensus_key] = $arr_key;
					}
				}
				
				//Find out the maximal value in consensus key_count
				$max = -1;
				$max_key = null;
				
				foreach($consensus_key_count as $count_key => $value)
				{
					if($max < $value + 0)
					{
						$max = $value + 0;
						$max_key = $count_key;
					}
				}
				
				//$max_key is actually the  md5(serialize($data)) that corresponds to
				//the most frequently occuring data
				
				//proceed to obtain the data index value from $consensus_key_map
				if($max > -1)
				{
					$fin_data = $data_arr[$consensus_key_map[$max_key]];
				}
				
				break;
		}
		return $fin_data;
	}
	
	/**
	 * @param string $key The key string based on which replicating servers are chosen
	 * @param mixed $extra This parameter is passed on to PrepareServerGroupForKey and ReadData methods
	 * @param string $tsCol The time-stamp column name, defaults to 'ts'
	 * @return mixed
	 * @desc Gets data from the replicating servers, assembles them into one coherent package based on the assembly method specified
	 * while instantiating the class object. Before use please override <strong>ReadData</strong> method.
	 */
	function Get($key, $extra = null, $tsCol = null)
	{
		$server_group = array();
		if($this->PrepareServerGroupForKey($key, $extra, $server_group))
		{
			$mega_data_arr = array();
			/*
			foreach($this->connected_servers_list as $server_index)
			{
				$mega_data_arr[$server_index+0] = $this->ReadData($server_index+0, $key, (empty($extra)?$this->extra:$extra));
			}
			*/
			foreach($server_group as $server_index)
			{
				$mega_data_arr[$server_index+0] = $this->ReadData($server_index+0, $key, (empty($extra)?$this->extra:$extra));
			}
			
			//print_r($mega_data_arr);
			
			return $this->AssembleData($mega_data_arr, $this->assemble_data_method, empty($tsCol)?$this->ts_col:$tsCol);
		}
		else
		{
			return null;
		}
	}
	
	/**
	 * @param string $key The key string based on which replicating servers are chosen
	 * @param mixed $data The data to write, can be an array
	 * @param mixed $extra This parameter is passed on to PrepareServerGroupForKey and WriteData methods
	 * @return boolean Returns true if write was successful, false otherwise
	 * @desc Writes data to the set of replicating servers. Before use please override <strong>WriteData</strong> method.
	 */
	function Set($key, $data, $extra = null)
	{
		$server_group = array();
		if($this->PrepareServerGroupForKey($key, $extra, $server_group))
		{
			/*
			foreach($this->connected_servers_list as $server_index)
			{
				$this->WriteData($server_index+0, $key, $data, (empty($extra)?$this->extra:$extra));
			}
			*/
			foreach($server_group as $server_index)
			{
				$this->WriteData($server_index+0, $key, $data, (empty($extra)?$this->extra:$extra));
			}
			
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * @param string $key The key string based on which replicating servers are chosen
	 * @param mixed $extra This parameter is passed on to PrepareServerGroupForKey and DeleteData methods
	 * @return boolean Returns true if deletion was successful, false otherwise.
	 * @desc Deletes data from the replicating servers, please override <strong>DeleteData</strong> method before use of this method.
	 */
	function Del($key, $extra = null)
	{
		$server_group = array();
		if($this->PrepareServerGroupForKey($key, $extra, $server_group))
		{
			foreach($server_group as $server_index)
			{
				$this->DeleteData($server_index, $key, (empty($extra)?$this->extra:$extra));
			}
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * @param string $key
	 * @param string $methodname The name of the method that is being called
	 * @param mixed $arguements The set of arguements to be passed to the method, this has to be an array. This array is later merged into an array containing the $key
	 * and $server_index values. The merged array is later used in a call_user_func call to execute the method.
	 * @param boolean $expects_return A value of true specifies that some return data is expected from the method called.
	 * @param integer $aggregation_algo This has to be an ASSEMBLE_* constant, default is ASSEMBLE_CONSENSUS. If expects_return is set to true, this method will be used
	 * to assemble the data obtained from the called method.
	 * @param mixed $extra
	 * @return mixed
	 * @desc A function to call an user defined method on the group of replicating servers for a particular $key. The target method receives the $key and individual 
	 * $server_index on which it is being called as the first two parameters. Extra parameters can be added via $arguements array.
	 */
	function CallMethodForKey($key, $methodname, $arguements = null, $expects_return = true, $aggregation_algo = ASSEMBLE_CONSENSUS, $extra = null)
	{
		
		if(method_exists($this, $methodname))
		{
			
			$server_group = array();
			if($this->PrepareServerGroupForKey($key, $extra, $server_group))
			{
				$mega_data_arr = array();
				
				foreach($server_group as $server_index)
				{
					$all_arguements = array($key, $server_index);
					
					if(!empty($arguements))
					{
						if(is_array($arguements))
						{
							$all_arguements = array_merge($all_arguements, $arguements);
						}
						else
						{
							$all_arguements[] = $arguements;
						}
					}
					
					try
					{
						$mega_data_arr[$server_index+0] = call_user_func_array(array($this, $methodname), $all_arguements);
					}
					catch(Exception $e)
					{
						$mega_data_arr[$server_index+0] = null;
					}
				}
				
				if($expects_return)
				{
					return $this->AssembleData($mega_data_arr, $aggregation_algo);
				}
			}
			
		}
		return null;
	}
}


/*
 * Test extension of SClientSharding 
 * 
 */
/*
class STestClientSharding extends SClientSharding
{
	function ReadData($server_index, $key, $extra = null)
	{
		if(rand()%3)
			return array('11');
		else
			return null;
	}
	
	function WriteData($server_index, $key, $data, $extra = null)
	{
		return true;
	}
	
	function DeleteData($server_index, $key, $extra = null)
	{
		return true;
	}
}

$STestClientSharding = new STestClientSharding(array('1', '2', '3', '4', '5'), 3);

$STestClientSharding->Set('blah', 'no good');

print_r($STestClientSharding->Get('blah'));
*/
?>