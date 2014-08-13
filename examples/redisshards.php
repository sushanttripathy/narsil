<?php
/**
 * Created by PhpStorm.
 * User: Sushant
 * Date: 8/13/14
 * Time: 2:47 AM
 */

include_once __DIR__."/../redis/sredisshards.php";

/*
 * Since Redis is a key-value store, the sharding is easily set-up according to the key
 */

$R = new SRedisShards(null, 2);

$R->simpleset('incr_test', 1);//Here the value is not serialized and can be used for atomic operations such as increment
echo $R->simpleget('incr_test');//Getting the value of a non-serialized key

$R->incr('incr_test');//Incrementing the value of a "simplest" non-serialized key by 1
$R->incrby('incr_test', 2);//Incrementing the value of the key by 2

$R->Set('ser_key', array('blah'=>'do'));//This key value is serialized
print_r($R->Get('ser_key'));//Get the value of a serialized key

if($R->exists('incr_test'))
{
    $R->Del('incr_test');
}

if($R->exists('ser_key'))
{
    $R->expire('ser_key', 10);//The key will expire after 10 seconds
}
