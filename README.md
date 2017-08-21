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


