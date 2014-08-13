<?php

/**
 * @author Sushant
 * @desc When extending Static_Class do not override the construct, instead just override the public static function InitInstance
 * and call the parental InitInstance function in the new overriden function. Before any other static function calls just perform
 * a self:InitInstance().
 *
 */
class Static_Class
{
	//protected static $instance = null;
	protected static $instances = array();
	
	public static function InitInstance()
	{
		/*
		if(empty(self::$instance))
		{
			self::$instance = new self();
		}
		return self::$instance;
		*/
		/*
		if(!isset(static::$instance) || empty(static::$instance)) 
		{
			static::$instance = new static();
		}
		
		return static::$instance;
		*/
		$called_class = get_called_class();
		if(!isset(self::$instances[$called_class]) || empty(self::$instances[$called_class]))
		{
			self::$instances[$called_class] = new static();
		}
		return self::$instances[$called_class];
	}
	
	private function __construct()
	{
		
	}
	
	function __destruct()
	{
		
	}
	
}

?>