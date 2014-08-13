narsil
======

A pure PHP implementation of client side sharding for MySQL, MongoDB and Redis. 

The base client sharding class can be found in clients\sclientsharding.php, which relies on the extension of the client skelton class (found in clients\sclientskeleton.php) for carrying out read/write/delete operations

Extended classes include SMySQLiShards (found in mysqli\smysqlishards.php), SMongoShards (found in mongodb\smongoshards.php) and SRedisShards (found in redis\sredisshards.php).

Before using these classes, it is recommended to specify the list of servers in the following files : 

1. For MySQL specify the servers in mysqli\mysql_servers.php (as per the format mentioned therein)
2. For MongoDB servers mongodb\mongodb_servers.php
3. For Redis servers redis\redis_servers.php

Examples of using the client side sharded classes can be found in the examples folder.
