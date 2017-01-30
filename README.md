# XenForo-MysqlReplication

Master/slave (with N slaves) replication while being slightly smarter than checking if the query starts with 'select'.

Connects to the slave(s) 1st, and then talks to master if required.

Note; any attributes* not in the slave config are pulled form the master config.

*(host,port,username,password,dbname)

Note; the addon XML file allows the 

## Master-Slave
The foundation for establishing multiple connections and switching from the slave to the master when rewrited on a write.

### Config:
- ```$config['db']['adapter'] = 'Masterslave';```

## Multi-Master
Extends master/slave to allow any slave to recieve xf_session_activity writes.

### Config:
- ```
$config['db']['adapter'] = 'Multimaster';
```

## Galera
Extends Multi-Master to dynamically load the cluster topology map. Writes are sent to the primary compontent, and other masters recieve xf_session_activity writes.

The slaves (or master) become a discovery list.

Requires:
- information_schema must be enabled
Recommended:
- APCu must be installed & enabled.

### Config:
- ```
$config['db']['adapter'] = 'Galera';
// how often to check for down nodes. (in seconds) Highly racey with last checker winning. 
$config['db']['galera']['discovery'] = 5;
$config['db']['galera']['apcu_prefix'] = ''; // apcu cache prefix keys
```


# Example config:
```
$config['db']['host'] = '127.0.0.1';
$config['db']['port'] = '3306';
$config['db']['username'] = 'username';
$config['db']['password'] = 'pass';
$config['db']['dbname'] = 'db';

$config['db']['adapter'] = 'Masterslave'; // or Multimaster/Galera
$config['db']['adapterNamespace'] = 'SV_MysqlReplication';
$config['db']['strictMode'] = true; // ensures strictmode is set on each connection, set to false if this has been configured in the database itself
$config['db']['public_max_statement_time'] = 3; // SQL timeout (in seconds for MariaDb) to apply to public (ie not admin) pages
$config['db']['master']['initialTransactionlevel'] = 'READ COMMITTED';// if set, use this transaction isolation level on the master
$config['db']['master']['transactionTransactionlevel'] = 'REPEATABLE READ'; // if set, use this transaction isolation level on the master and starting a transaction
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
