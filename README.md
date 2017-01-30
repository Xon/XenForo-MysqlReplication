# XenForo-MysqlReplication

Master/slave (with N slaves) replication while being slightly smarter than checking if the query starts with 'select'.

Connects to the slave(s) 1st, and then talks to master if required.

Note; any attributes* not in the slave config are pulled form the master config.

*(host,port,username,password,dbname)

Example config:
```
$config['db']['host'] = '127.0.0.1';
$config['db']['port'] = '3306';
$config['db']['username'] = 'username';
$config['db']['password'] = 'pass';
$config['db']['dbname'] = 'db';

$config['db']['adapter'] = 'Masterslave';
$config['db']['adapterNamespace'] = 'SV_MysqlReplication';
$config['db']['strictMode'] = true; // ensures strictmode is set on each connection, set to false if this has been configured in the database itself
$config['db']['slaves'] = array(
/*
    array(
        'host' => '127.0.0.1',
        'port' = '3307',
    ),
    array(
        'host' => '127.0.0.1',
        'port' => '3308',
    ),
*/    
);
```
