<?php

include_once __DIR__.'/sredis.php';
include_once __DIR__.'/../clients/sclientsharding.php';

define('__1_ENABLE_DEBUG', true);

/*
 * Note: Do not combine incr or incrby  and Set in the Shards, the Shards serialize values, incr and incrby don't and
 * cannot handle serialized values. However they can be used with exists and expire. Also they can be used with simpleset
 * and simpleget. Also note that simpleset and simpleget are not compatible with Get, Set or Del. exists and expire are
 * compatible with everything.
 */

/*
class SRedisShards
{
	protected $servers;
	protected $indices;
	protected $RedisConnections;
	protected $replicas = 0;
	protected $lastSearchedIndex = 0;
	
	protected $totalServerCount = 0;
	
	function __construct($servers = null, $replicas = 2)
	{
		if(empty($servers))
		{
			$this->servers = $GLOBALS['REDIS_SERVERS'];
			$this->totalServerCount = TOTAL_REDIS_SERVER_COUNT;
		}
		else 
		{
			$this->servers = $servers;
			$this->totalServerCount = count($servers);
		}
		$this->replicas = max($replicas, 1);
	}
	
	function GetCompareSubStringLength()
	{
		return floor(log($this->totalServerCount, 256)) + 1;
	}
	
	function DiscardUnusedConnections($oldRedisConnections, $newRedisConnections)
	{
		
	}
	
	function IsConnected($serverindex)
	{
		if(!empty($this->indices))
		{
			foreach($this->indices as $key => $value)
			{
				if($value == $serverindex)
				{
					$this->lastSearchedIndex = $key;
					return true;
				}
			}
		}
		
		$this->lastSearchedIndex = null;
		return false;

	}
	
	
	function GetLastSearchedConnection()
	{
		if(!empty($this->RedisConnections) && $this->lastSearchedIndex !== null)
		{
			return $this->RedisConnections[$this->lastSearchedIndex];
		}
		return null;
	}
	
	function PrepareServerGroupsForKey($key)
	{
		
		$j = $this->GetCompareSubStringLength();
		$finalvalue = 0;
		
		for($i = 0; $i < $j; $i++)
		{
			$finalvalue = $finalvalue+ord($key{$i})*pow(256, $i);
		}
		
		$totalExistingIndices = count($this->indices);
		
		$newIndices = array();
		$newRedisConnections = array();
		
		$firstIndex = $finalvalue%$this->totalServerCount;
		
		$maxCount = $firstIndex + min($this->replicas, $this->totalServerCount);
		
		for($i = $firstIndex; $i < $maxCount; $i++)
		{
			$index = $i%$this->totalServerCount;
			$newIndices[] = $index;
			
			if($this->IsConnected($index))
			{
				$newRedisConnections[] = $this->GetLastSearchedConnection();
			}
			else
			{
				$server = 'tcp://'.$GLOBALS['REDIS_SERVERS'][$index]['host'].':'.$GLOBALS['REDIS_SERVERS'][$index]['port'];
				
				try 
				{
					$newRedisConnections[] = new Predis\Client($server);
				}
				catch(Exception $e)
				{
					if(__1_ENABLE_DEBUG)
					{
						echo $e->getMessage();
					}
					array_pop($newRedisConnections);
					array_pop($newIndices);
					
					@end($newRedisConnections);
					@end($newIndices);
				}
			}
		}
		
		$this->DiscardUnusedConnections($this->RedisConnections,$newRedisConnections);
		
		$this->RedisConnections = $newRedisConnections;
		$this->indices = $newIndices;
	}
	
	function set($key, $data)
	{
		if(empty($data))
			return true;
			
		$tstamp = time();
		
		$this->PrepareServerGroupsForKey($key);
		
		$rdata = array('ts'=>$tstamp, 'data'=>$data);
		
		$allservers = count($this->RedisConnections);
		
		for($i = 0; $i < $allservers; $i++)
		{
			try 
			{
				$this->RedisConnections[$i]->set($key, serialize($rdata));
			}
			catch(Exception $e)
			{
				if(__1_ENABLE_DEBUG)
				{
					echo $e->getMessage();
				}
			}
		}
		return true;
	}
	
	function get($key)
	{
		if(empty($key))
			return null;
		
		$this->PrepareServerGroupsForKey($key);
	
		$allservers = count($this->RedisConnections);
		
		$data = array();
		
		$latest = 0;
		$latestIndex = null;
		
		$exists = 0;
		$notexists = 0;
		
		for($i = 0; $i < $allservers; $i++)
		{
			try 
			{
				$data[$i]= unserialize($this->RedisConnections[$i]->get($key));
			}
			catch(Exception $e)
			{
				$data[$i] = null;
				if(__1_ENABLE_DEBUG)
				{
					echo $e->getMessage();
				}
			}
			
			if(empty($data[$i]))
			{
				$notexists++;
			}
			else 
			{
				$exists++;
				if($data[$i]['ts'] > $latest )
				{
					$latest = $data[$i]['ts'];
					$latestIndex = $i;
				}
			}
		}
		
		if($exists >= $notexists && $latestIndex !== null)
		{
			return $data[$latestIndex]['data'];
		}
		else
		{
			return null;
		}
	}
	
	function exists($key)
	{
		if(empty($key))
			return null;
		
		$this->PrepareServerGroupsForKey($key);
		
		$allservers = count($this->RedisConnections);
		
		$data = array();
		
		$exists = 0;
		$notexists = 0;
		
		for($i = 0; $i < $allservers; $i++)
		{
			try 
			{
				$entry =$this->RedisConnections[$i]->exists($key);
			}
			catch (Exception $e)
			{
				$entry = null;
				if(__1_ENABLE_DEBUG)
				{
					echo $e->getMessage();
				}
			}
			
			if($entry)
			{
				$exists++;
			}
			else
			{
				$notexists++;
			}
		}
		
		if($exists > $notexists)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	function expire($key, $time)
	{
		if(empty($key))
			return null;
		
		$this->PrepareServerGroupsForKey($key);
		
		$allservers = count($this->RedisConnections);
		
		for($i = 0; $i < $allservers; $i++)
		{
			try 
			{
				$this->RedisConnections[$i]->expire($key, $time);
			}
			catch(Exception $e)
			{
				if(__1_ENABLE_DEBUG)
				{
					echo $e->getMessage();
				}
			}
		}
		return true;
	}
	
	function del($key)
	{
		if(empty($key))
			return null;
		
		$this->PrepareServerGroupsForKey($key);
		
		$allservers = count($this->RedisConnections);
		
		for($i = 0; $i < $allservers; $i++)
		{
			try 
			{
				$this->RedisConnections[$i]->del($key);
			}
			catch(Exception $e)
			{
				if(__1_ENABLE_DEBUG)
				{
					echo $e->getMessage();
				}
			}
		}
		return true;
	}
}
*/

class SRedisShards extends SClientSharding
{
	function __construct($servers_arr = null, $replicas = 3)
	{
		if(empty($servers_arr))
		{
			$servers_arr = SRedisServers::GetServersInfo();
			
			@parent::__construct($servers_arr, $replicas, false, true, ASSEMBLE_LATEST, 'ts');
		}
	}
	
	function ConnectToServer($server_index, $extra = null, $established_connection = null)
	{
		$server_details = $this->servers_list[$server_index];
		
		if(!empty($server_details))
		{
			$connection_obj = new SRedis($server_details['host'], $server_details['port']);
				
			@parent::ConnectToServer($server_index, null, $connection_obj);
			return true;
		}
		return false;
	}
	
	function ReadData($server_index, $key, $extra = null)
	{
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return unserialize($connection_obj->get($key));
		}
		return null;
	}
	
	function WriteData($server_index, $key, $data, $extra = null)
	{
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->set($key, serialize($data));
		}
		return false;
	}
	
	function DeleteData($server_index, $key, $extra = null)
	{
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->del($key);
		}
		return false;
	}
	
	/*
	function get($key)
	{
		return $this->Get($key);
	}
	
	function del($key)
	{
		return $this->Del($key);
	}
	
	function set($key, $data)
	{
		return $this->Set($key, $data);
	}
	*/
	
	function exists($key)
	{
		return $this->CallMethodForKey($key, '_Exists');
	}

	function expire($key, $time)
	{
		return $this->CallMethodForKey($key, '_Expire', $time, false);
	}

    function incr($key)
    {
        return $this->CallMethodForKey($key, '_Incr');
    }

    function incrby($key, $increment)
    {
        return $this->CallMethodForKey($key, '_Incrby', $increment);
    }

    function simpleget($key)
    {
        return $this->CallMethodForKey($key, '_Simpleget', null, true, ASSEMBLE_CONSENSUS);
    }

    function simpleset($key, $value)
    {
        return $this->CallMethodForKey($key, '_Simpleset', $value, true, ASSEMBLE_CONSENSUS);
    }
	
	function Get($key, $extra = null)
	{
		$res = @parent::Get($key, $extra );
		
		if(!empty($res))
		{
			return $res['data'];
		}
		return null;
	}
	
	function Set($key, $data, $extra = null)
	{
		$data_arr = array('ts'=>time(), 'data'=>$data);
		return @parent::Set($key, $data_arr, $extra);
	}
	
	function Del($key, $extra = null)
	{
		return @parent::Del($key, $extra);
	}
	
	protected function _Exists($key, $server_index)
	{
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->exists($key);
		}
		return false;
	}
	
	protected function _Expire($key, $server_index, $time)
	{
		$connection_obj = $this->connections_list[$server_index];
		
		if(!empty($connection_obj) && is_object($connection_obj))
		{
			return $connection_obj->expire($key, $time);
		}
		return false;
	}

    protected function _Incr($key, $server_index)
    {
        $connection_obj = $this->connections_list[$server_index];

        if(!empty($connection_obj) && is_object($connection_obj))
        {
            return $connection_obj->incr($key);
        }
        return false;
    }

    protected function _Incrby($key, $server_index, $increment)
    {
        $connection_obj = $this->connections_list[$server_index];

        if(!empty($connection_obj) && is_object($connection_obj))
        {
            return $connection_obj->incrby($key, $increment);
        }
        return false;
    }

    protected function _Simpleget($key, $server_index)
    {
        $connection_obj = $this->connections_list[$server_index];

        if(!empty($connection_obj) && is_object($connection_obj))
        {
            return $connection_obj->simpleget($key);
        }
        return null;
    }

    protected function _Simpleset($key, $server_index, $value)
    {
        $connection_obj = $this->connections_list[$server_index];

        if(!empty($connection_obj) && is_object($connection_obj))
        {
            return $connection_obj->simpleset($key, $value);
        }
        return false;
    }
}
?>