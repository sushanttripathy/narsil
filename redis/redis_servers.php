<?php

include_once  __DIR__."/../factory/static_class.php";

$redis_servers = array();

$redis_servers[0] = array('host'=>'127.0.0.1', 'port'=>6379);

define('TOTAL_REDIS_SERVER_COUNT', count($redis_servers));

class SRedisServers extends Static_Class
{
    protected static $servers_info;


    public static function InitInstance()
    {
        if(empty(self::$servers_info))
        {
            self::$servers_info = array();
        }
        return @parent::InitInstance();
    }

    public static function GetServersInfo()
    {
        return self::$servers_info;
    }

    public static function SetServersInfo($servers_info_ = array())
    {
        self::$servers_info = $servers_info_;
    }
}

SRedisServers::SetServersInfo($redis_servers);

?>