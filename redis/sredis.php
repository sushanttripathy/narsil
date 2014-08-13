<?php

include_once __DIR__.'/redis_servers.php';

include_once __DIR__.'/../external/predis/vendor/autoload.php';

include_once __DIR__.'/../clients/sclientskeleton.php';
//include_once '/srv/includes/redis_servers.php';

class SRedis extends SClientSkeleton
{
	function __construct($host, $port)
	{
		@parent::__construct(array($host, $port), null, true);
	}
	
	function ConnectToServer()
	{
		$server = $server = 'tcp://'.$this->server_details[0].':'.$this->server_details[1];
		$RedisObj = new Predis\Client($server);
		
		return $RedisObj;
	}
	
	function IsConnected($RedisObj)
	{
        //print_r($RedisObj->isConnected());
		return $RedisObj->isConnected();
	}
	
	function o_set($key, $data)
	{
		$RedisObj = $this->GetOrMakeServerConnection();
		return $RedisObj->set($key, $data);
	}
	
	function o_get($key)
	{
		$RedisObj = $this->GetOrMakeServerConnection();
		return $RedisObj->get($key);
	}
	
	function o_expire($key, $time)
	{
		$RedisObj = $this->GetOrMakeServerConnection();
		return $RedisObj->expire($key, $time);
	}
	
	function o_exists($key)
	{
		$RedisObj = $this->GetOrMakeServerConnection();
		return $RedisObj->exists($key);
	}
	
	function o_del($key)
	{
		$RedisObj = $this->GetOrMakeServerConnection();
		return $RedisObj->exists($key);
	}

    function o_incr($key)
    {
        $RedisObj = $this->GetOrMakeServerConnection();
        return $RedisObj->incr($key);
    }

    function o_incrby($key, $increment)
    {
        $RedisObj = $this->GetOrMakeServerConnection();
        return $RedisObj->incrby($key, $increment);
    }

    function o_simpleget($key)
    {
        $RedisObj = $this->GetOrMakeServerConnection();
        return $RedisObj->get($key);
    }

    function o_simpleset($key, $value)
    {
        $RedisObj = $this->GetOrMakeServerConnection();
        return $RedisObj->set($key, $value);
    }
}