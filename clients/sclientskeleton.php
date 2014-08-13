<?php

class SClientSkeleton
{
    const CONNECTION_WAIT_TIME = 50000; //microseconds
    const MAX_CONNECTION_ATTEMPTS = 2;

	protected $server_details = null;
	protected $make_static_pool = false;
	
	protected static $static_connections_pool = array();
	
	protected $connection = null;
	protected $requires_lost_connection_cleanup = false;
	
	function __construct($server_details = array(), $extra_params = null, $make_static_pool = false, $requires_lost_connection_cleanup = false)
	{
		$this->server_details = $server_details;
		$this->make_static_pool = $make_static_pool;
		$this->requires_lost_connection_cleanup = $requires_lost_connection_cleanup;
	}
	
	function SetServerDetails($server_details)
	{
		if(!empty($server_details))
		{
			$this->server_details = $server_details;
			return true;
		}
		return false;
	}
	
	function GetOrMakeServerConnection()
	{
		$calling_class = get_called_class ();
		
		$ret = null;
		if($this->make_static_pool)
		{
			//check if the connection is present in the static pool
			$index = md5(serialize($this->server_details));
			
			if(empty(self::$static_connections_pool) || empty(self::$static_connections_pool[$calling_class]) ||empty(self::$static_connections_pool[$calling_class][$index]) || !$this->SafeIsConnected(self::$static_connections_pool[$calling_class][$index]))
			{
                $try_number = 0;
                $done = false;
                do
                {
                    if(isset(self::$static_connections_pool[$calling_class][$index]) && !empty(self::$static_connections_pool[$calling_class][$index]) && $this->requires_lost_connection_cleanup)
                    {
                        $this->SafeCleanupLostConnection(self::$static_connections_pool[$calling_class][$index]);
                        unset(self::$static_connections_pool[$calling_class][$index]);
                    }
                    //Not present hence make the connection
                    $connection = $this->SafeConnectToServer();

                    if(!$this->SafeIsConnected($connection))
                    {
                        usleep(self::CONNECTION_WAIT_TIME);
                        $try_number++;
                    }
                    else
                    {
                        $done = true;
                    }
                }while($try_number < self::MAX_CONNECTION_ATTEMPTS && !$done );

				$index = md5(serialize($this->server_details));
				
				self::$static_connections_pool[$calling_class][$index] = $connection;
			}
			$ret = self::$static_connections_pool[$calling_class][$index];
		}
		else
		{
			if(empty($this->connection) || !$this->SafeIsConnected($this->connection))
			{
				if(!empty($this->connection) && $this->requires_lost_connection_cleanup)
				{
					$this->SafeCleanupLostConnection($this->connection);
				}
				//Make connection
                $try_number = 0;
                $done = false;
                do
                {
				    $this->connection = $this->SafeConnectToServer();
                    if(!$this->SafeIsConnected($this->connection))
                    {
                        usleep(self::CONNECTION_WAIT_TIME);
                        $try_number++;
                    }
                    else
                    {
                        $done = true;
                    }
                }while($try_number < self::MAX_CONNECTION_ATTEMPTS && !$done);

			}
			
			$ret = $this->connection;
		}
		
		return $ret;
	}
	
	
	function IsConnected($param)
	{
		return true;
	}
	
	function CleanupLostConnection($param)
	{
		return true;
	}
	
	function ConnectToServer()
	{
		$connection_object = null;
		return $connection_object;
	}
	
	protected function SafeConnectToServer()
	{
		$ret = null;
		try {
			$ret = $this->ConnectToServer();
		} catch (Exception $e) {
			$ret = null;
		}
		return $ret;
	}
	
	protected function SafeCleanupLostConnection($param)
	{
		$ret = false;

		try {
			$ret = $this->CleanupLostConnection($param);
		}
		catch(Exception $e)
		{
			$ret = false;
		}
		return $ret;
	}
	
	protected function SafeIsConnected($param)
	{
		$ret = false;
		
		try {
			$ret = $this->IsConnected($param);
		}
		catch(Exception $e)
		{
			$ret = false;
		}
		return $ret;
	}
	
	protected function GetExistingConnectionFromPool($server_details)
	{
		$calling_class = get_called_class ();
		$index = md5(serialize($this->server_details));
		
		if(isset(self::$static_connections_pool[$calling_class][$index]) && !empty(self::$static_connections_pool[$calling_class][$index]))
		{
			return self::$static_connections_pool[$calling_class][$index];
		}
		return null;
	}
	
	function __call($method, $arguements)
	{
		$ret = null;
		if(method_exists($this, 'o_'.$method))
		{
			$real_method_name = 'o_'.$method;
			
			
			try {
				$ret = call_user_func_array(array($this, $real_method_name), $arguements);
			}
			catch(Exception $e)
			{
				$ret = null;
			}
			return $ret;
			
		}
		else 
		{
			$ConnObj = $this->GetOrMakeServerConnection();
			
			if(is_object($ConnObj))
			{
				if(method_exists($ConnObj, $method))
				{
					try {
						$ret = call_user_func_array(array($ConnObj, $method), $arguements);
					}
					catch (Exception $e)
					{
						$ret = null;
					}
				}
			}
		}
		return $ret;
	}
	
	function __get($property)
	{
		$ret = null;
		if(property_exists($this, $property))
		{
			$ret = $this->$property;
		}
		else
		{
			$ConnObj = $this->GetOrMakeServerConnection();
			
			if(is_object($ConnObj))
			{
				if(property_exists($ConnObj, $property))
				{
					try {
						$ret = $ConnObj->$property;
					}
					catch (Exception $e)
					{
						$ret = null;
					}
				}
			}
		}
		
		return $ret;
	}
	
	/*
	 * function o_Set($key, $data)
	 * {
	 * 		$connection_obj = $this->GetOrMakeServerConnection();
	 * 		$ret = $connection_obj->Set($key, $data);
	 * 		return $ret;
	 * }
	 */
}