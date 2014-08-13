<?php

define('DEFAULT_ID_TABLE', 'generate_id');



include_once  __DIR__.'/../mysqli/smysqli.php';
//include_once '../factory/static_class.php';

class SGenerateID
{
	protected $table=null;
	protected $SMySQLi = null;
	
	function __construct($table=DEFAULT_ID_TABLE)
	{
		$this->table = $table;
	}
	
	function InitMySQL()
	{
		if(empty($this->SMySQLi))
		{
			$this->SMySQLi = new SMySQLiv2();
		}
	}

    function Setup()
    {
        $query = 'CREATE TABLE IF NOT EXISTS `'.$this->table.'` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `stub` bigint(20) NOT NULL, PRIMARY KEY (`id`), UNIQUE KEY `stub_` (`stub`))';
        $this->InitMySQL();

        $all_servers = SMySQLiServers::GetServersInfo();

        foreach($all_servers as $server_info)
        {
            $SMySQLi = new SMySQLiv2($server_info['host'], $server_info['user'], $server_info['pass'], $server_info['db'], $server_info['port']);

            $SMySQLi->query($query);
        }

        return true;
    }
	
	function getNewID()
	{
		$query = 'REPLACE INTO `'.$this->table.'` (stub) VALUE(\'1\')';
		$this->InitMySQL();
		
		if($this->SMySQLi->query($query))
		{
			return TOTAL_MYSQL_SERVERS*($this->SMySQLi->insert_id - 1) + $this->SMySQLi->GetServerIndex()+1;
		}
		else
		{
			return null;
		}
	}
}


class StaticSGenerateID extends Static_Class
{
    protected static $SGenerateID;

    public static function InitInstance()
    {
        if(empty(self::$SGenerateID))
        {
            self::$SGenerateID = new SGenerateID();
        }
        return @parent::InitInstance();
    }

    public static function getNewID()
    {
        self::InitInstance();
        return self::$SGenerateID->getNewID();
    }

    public static function Setup()
    {
        self::InitInstance();
        return self::$SGenerateID->Setup();
    }
}

?>