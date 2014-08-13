<?php


include_once  __DIR__."/../factory/static_class.php";


$mongo_servers = array();
$mongo_servers[] = array('host'=>'127.0.0.1', 'port'=>27017, 'user'=>'', 'pass'=>'');


class SMongoServers extends Static_Class
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

SMongoServers::SetServersInfo($mongo_servers);

?>