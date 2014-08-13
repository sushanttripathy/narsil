<?php
/**
 * Created by PhpStorm.
 * User: Sushant
 * Date: 8/13/14
 * Time: 2:29 AM
 */

include_once __DIR__."/../mysqli/smysqlishards.php";

/*
 * Before using the SMySQLiShards class it is necessary to setup the table that the sharded client class would use.
 * The table must contain a bigint "id" field which is set as PRIMARY_KEY, but not AUTO_INCREMENT enabled. Additionally
 * the table should also contain a bigint "ts" field.
 *
 * Those apart it is necessary to setup a table for generating unique numeric ids. This can be achieved by running the
 * following lines only once in the lifetime of the database.
 *
 * $SG = new SGenerateID();
 * $SG->Setup();
 */



$S = new SMySQLiShards(null, 'sharded_table', 2);

$sharding_key = "blah"; //this is the key which determines the sharding across servers in the methods below

$S->Set(array('field_x'=>11), array('field_y'=>rand(0,100)), $sharding_key);//The first array specifies the condition
                                                                            //to search for, if an item satisfying the
                                                                            //condition does not exist in the table,
                                                                            //it is added to the  data field and a new
                                                                            //item is created.
                                                                            //The second array specifies the data that
                                                                            //is to be INSERTED or UPDATED (if the item
                                                                            //as identified by the condition array
                                                                            //exists)
                                                                            //The third parameter specifies the key to
                                                                            //be used for sharding

$data = $S->Get(array('field_x' => 11), $sharding_key);//Searching for the item by condition (the first array)

$S->Delete(array('field_x'=>11), 'id', $sharding_key);//Deleting the item by searching for condition (the first array)

$S->Set(array('field_x'=>11), array('field_y'=>rand(0,100)), $sharding_key);//Re-inserting the item

$data = $S->Get(array('field_x' => 11), $sharding_key);

$S->DeleteById($data['id'], 'id', $sharding_key );//Deleting the item by id (first parameter)
