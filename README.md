# XenForo-MysqlReplication

Master/slave (with N slaves) replication while being slightly smarter than checking if the query starts with 'select'.

$config['db']['adapter'] = 'MasterSlave';
$config['db']['adapterNamespace'] = 'SV_MysqlReplication';
$config['db']['slaves'] = array(
/*
    array(
        'host' => '127.0.0.1',
        'port' = '3306',
        'username' => 'username',
        'password' => 'pass',
        'dbname' => 'db',
    ),
    array(
        'host' => '127.0.0.1',
        'port' => '3307',
        'username' => 'username',
        'password' => 'pass',
        'dbname' => 'db',
    ),
*/    
);
