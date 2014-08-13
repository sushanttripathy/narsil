<?php

include_once  __DIR__.'/mongodb_servers.php';
class SMongo
{
	protected $conn = null;
	protected $db = null;
	protected $column = null;
	
	
	function __construct($server = null, $db = 'default', $column = 'default')
	{
		if(empty($server))
		{
            $servers = SMongoServers::GetServersInfo();
			if(!empty($servers))
			{
				$server_inf = $servers[rand()%count($servers)];
                $server = "mongodb://".$server_inf['host'].":".$server_inf['port'];
			}
		}
		try 
		{
			$this->conn = new Mongo($server);
			$this->db = $this->conn->$db;
			$this->column = $this->db->$column;
		}
		catch(Exception $e)
		{
			$this->conn = null;
			$this->db = null;
			$this->column = null;
			echo $e->getMessage();
		}
		
	}
	
	function __destruct()
	{
		//echo "Destroying Mongo\n";
	}
	
	function GetID($condtion)//$condition is an array()
	{
		
	}
	
	function GetByID($id)
	{
		
	}
	
	function Get($condtion)//$condition is an array()
	{
		if($this->column)
		{
			try 
			{
				return $this->column->find($condtion);
			}
			catch (Exception $e)
			{
				echo $e->getMessage();
			}
		}
		return null;
	}
	
	function GetOne($condition, $filter = null)
	{
		if($this->column)
		{
			try 
			{
				if(empty($filter))
					return $this->column->findOne($condition);
				else
					return $this->column->findOne($condition,$filter);
			}
			catch (Exception $e)
			{
				echo $e->getMessage();
			}
		}
		return null;
	}
	
	function Set($condition, $set)
	{
		return $this->StrictSet($condition, $set);
	}
	
	function Push($condition, $set)
	{
		$success = true;
		if($this->column)
		{
			try 
			{
				$this->column->update($condition, array('$push'=>$set), array("upsert" => true,"safe"=>true));
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
				$success = false;
			}
		}
		return $success;
	}
	
	function InclusiveSet($condition, $set)//$condition is an array(), $set is an array()
	{
		$success = true;
		if($this->column)
		{
			try 
			{
				$this->column->update($condition, array('$addToSet'=>$set), array("upsert" => true,"safe"=>true));
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
				$success = false;
			}
		}
		return $success;
	}
	
	
	function StrictSet($condition, $set)
	{
		$success = true;
		if($this->column)
		{
			try 
			{
				$this->column->update($condition, array('$set'=>$set), array("upsert" => true, "safe"=>true));
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
				$success = false;
			}
		}
		return $success;
	}
	
	function SimpleSet($condition, $set)
	{
		if($this->column)
		{
			try 
			{
				$this->column->update($condition, $set, array("upsert" => true));
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
			}
			
		}
	}
	
	function Increment($condition, $set)
	{
		if($this->column)
		{
			try 
			{
				$this->column->update($condition, array('$inc'=>$set), array("upsert" => true));
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
			}	
		}
	}
	
	function Insert($set)
	{
		if($this->column)
		{
			try 
			{
				$this->column->insert($set);
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
			}
			
		}
	}
	
	function SetByID($id, $set)//$set is an array()
	{
		
	}
	
	function SetDB($db)
	{
		if($this->conn)
		{
			try 
			{
				$this->db = $this->conn->$db;
				$this->column = null;
			}
			catch(Exception $e)
			{
				$this->db = null;
				echo $e->getMessage();
			}
		}
	}
	
	function SetColumn($column)
	{
		if($this->db)
		{
			try 
			{
				$this->column = $this->db->$column;
			}
			catch(Exception $e)
			{
				$this->column = null;
				echo $e->getMessage();
			}
		}
	}
	
	function Delete($condition, $delSet)//$condition is an array(), $delSet is a string
	{
		if($this->column)
		{
			try 
			{
				$this->column->update($condition, array('$unset'=>array($delSet=>1)));
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
			}
		}
	}
	
	function Remove($condition)
	{
		if($this->column)
		{
			try 
			{
				$this->column->remove($condition);
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
			}
		}
	}
	
	function GetLastError()
	{
		$outp = '';
		if($this->db)
		{
			try 
			{
				$outp = $this->db->lastError();
			}
			catch(Exception $e)
			{
				echo $e->getMessage();
			}
		}
		return $outp;
	}
}

?>