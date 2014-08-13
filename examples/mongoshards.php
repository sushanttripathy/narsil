<?php
/**
 * Created by PhpStorm.
 * User: Sushant
 * Date: 8/13/14
 * Time: 4:27 AM
 */

include_once __DIR__."/../mongodb/smongoshards.php";

$M = new SMongoShards(null, 'testdb', 'testcol', 2);

$sharding_key = 'arbitrary_key';//this is the key which determines the sharding across servers in the methods below

$M->Set(array('field_x' => 11), array('field_y' => 12), $sharding_key); //The first array specifies the condition
                                                                        //to search for, if an item satisfying the
                                                                        //condition does not exist in the column,
                                                                        //it is added to the  data field and a new
                                                                        //item is created.
                                                                        //The second array specifies the data that
                                                                        //is to be INSERTED or UPDATED (if the item
                                                                        //as identified by the condition array
                                                                        //exists)
                                                                        //The third parameter specifies the key to
                                                                        //be used for sharding
$M->Set(array('field_x' => 12), array('field_y' => 12), $sharding_key);

print_r($M->Get(array('field_y'=>12), $sharding_key));//Gets a list of all items that match the condition

print_r($M->GetOne(array('field_x'=>12), $sharding_key));//Gets only one item that matches the condition

$M->Delete(array('field_x'=>11), array('field_y' => 1), $sharding_key);//Deletes the field specified in the second array
                                                                       //from item(s) that match the condition (first) array

$M->Remove(array('field_y'=>12), $sharding_key);//Deletes the item(s) that match the condtion (first) array
