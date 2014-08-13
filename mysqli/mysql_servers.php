<?php

include_once  __DIR__."/../factory/static_class.php";



$mysqliservers[0]['host'] = '127.0.0.1';
$mysqliservers[0]['port'] = '3306'; //ini_get("mysqli.default_port");
$mysqliservers[0]['user'] = 'test';
$mysqliservers[0]['pass'] = 'something_something_darkside';
$mysqliservers[0]['db']   = 'test';

define ('TOTAL_MYSQL_SERVERS', count($mysqliservers));

class SMySQLiServers extends Static_Class
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

SMySQLiServers::SetServersInfo($mysqliservers);

?>