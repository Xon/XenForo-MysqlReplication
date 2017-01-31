<?php

class SV_MysqlReplication_Multimaster extends SV_MysqlReplication_Masterslave
{
    protected $topology = null;
    protected $prefix = null;

    public function __construct($config)
    {
        parent::__construct($config);
        $this->prefix = empty($config['galera']['apcu_prefix']) ? '' : $config['galera']['apcu_prefix'];
        $this->discovery = empty($config['galera']['discovery']) ? 5 : max(intval($config['galera']['discovery']), 1);
    }

    protected function _discoverTopology($hasConnection)
    {
        if ($this->topology !== null)
        {
            return;
        }
        $doDiscovery = true;
        $topology = false;
        $masterId = 0; // wsrep_incoming_addresses is sorted by node index

        // probe if we know the topology or need to re-probe for the topology
        if (function_exists('apcu_fetch'))
        {
            $topology = apcu_fetch($this->prefix . '.galera' );
            //$masterId = apcu_fetch($this->prefix . '.galera.master' );
            if ($topology !== false)
            {
                $doDiscovery = apcu_add($this->prefix . '.galera.check', 1, $this->discovery);
            }
        }

        $useTopology = true;
        if ($doDiscovery || $topology === false)
        {
/* -- verify and test, want a short timeout (ON CONNECT) on the assumption there is another node to use
ini_set('connect_timeout', $timeout);
ini_set('default_socket_timeout', $timeout);
ini_set('mysql.connect_timeout', $timeout);
ini_set('mysqlnd.net_read_timeout', $timeout);

echo 'PHP params:' . PHP_EOL;
msg('connect_timeout', ini_get('connect_timeout'));
msg('default_socket_timeout', ini_get('default_socket_timeout'));
msg('mysql.connect_timeout', ini_get('mysql.connect_timeout'));
msg('mysqlnd.net_read_timeout', ini_get('mysqlnd.net_read_timeout'));
*/

            $useTopology = false;
            $lastError = null;
            do
            {
                // remove this host from consideration
                if ($this->_master_config['host'] == $this->config['host'] && $this->_master_config['post'] == $this->config['post'])
                {
                    //TODO: master is dead :(
                }
                foreach($this->_slave_config as $key => &$connection)
                {
                    if ($connection['host'] == $this->config['host'] && $connection['post'] == $this->config['post'] )
                    {
                        unset($this->_slave_config[$key]);
                    }
                }
                // connect to any host. This is racy, but that is ok.
                try
                {
                    $this->_connect();
                }
                catch(Exception $e)
                {
                    $lastError = $e;
                    continue;
                }
                // 'wsrep_cluster_size', 'wsrep_local_index',
                $results = $this->_connection->query("
                     select * from information_schema.GLOBAL_STATUS where VARIABLE_NAME in ('wsrep_ready', 'wsrep_connected', 'wsrep_local_state_comment', 'wsrep_sst_method', 'wsrep_incoming_addresses');
                ");
                $row = $results->fetch_assoc();
                if (empty($row))
                {
                    $this->topology = explode(',', $row['wsrep_incoming_addresses']);
                    foreach($this->topology as &$node)
                    {
                        if (preg_match("/^(?:[0-9.]+|(?:\[[0-9a-fA-F:]+\]))(:[0-9]+)$/", $ip))
                        {
                            $node = $ip;
                        }
                        else
                        {
                            $node = array($node, 3306);
                        }
                    }
                    if (function_exists('apcu_store'))
                    {
                        apcu_store($this->prefix . '.galera', $this->topology);
                        //apcu_store($this->prefix . '.galera.master', $masterId);
                    }

                    $hostOk = false;
                    if ($row['wsrep_ready'] == 'ON' &&
                        $row['wsrep_connected'] == 'ON' &&
                        ($row['wsrep_local_state_comment'] == 'Synced' || ($row['wsrep_local_state_comment'] == 'Donor' && $row['wsrep_sst_method'] == 'xtrabackup'))
                    {
                        // host OK. try another node, exclude this one
                        $useTopology = true;
                        break;
                    }
                }
                // host not OK. try another node
            }
            //TODO: need to fix this loop condition
            while ( ... );

            if (!$useTopology)
            {
                $this->topology = null;
                // we can't find a valid entry point.
                throw new Exception("Discovery for galera cluster failed");
            }
        }

        if ($useTopology && $this->topology)
        {
            // rewrite the master
            $master =  isset($this->topology[$masterId]) ? $this->topology[$masterId] : reset($this->topology);
            $this->_master_config['host'] = $master['host'];
            $this->_master_config['post'] = $master['post'];
            // rewrite the slaves
            $this->_slave_config = array();
            foreach($this->topology as $hostPort)
            {
                $slave = array('host' => $hostPort[0], 'port' => $hostPort[1]);
                $this->copyAttributes($slave, $this->_master_config);
                $this->_slave_config[] = $slave;
            }
        }

        return;
    }

    protected function _connectMasterSetup()
    {
        if (!parent::_connectMasterSetup())
        {
            return false;
        }
        $this->_discoverTopology();

        return $this->_connection === null;
    }

    protected function _connectSlaveSetup($slaveId = null)
    {
        if (!parent::_connectSlaveSetup($slaveId))
        {
            return false;
        }
        $this->_discoverTopology();

        return $this->_connection === null;
    }

    //SHOW GLOBAL STATUS LIKE 'wsrep_local_index';
    //SHOW GLOBAL STATUS LIKE 'wsrep_cluster_size';
    //SHOW GLOBAL STATUS LIKE 'wsrep_cluster_status'; == 'Primary'
    //SHOW GLOBAL STATUS LIKE 'wsrep_ready'; == 'ON'
    //
    public function postConnect($writable)
    {
/*
        $this->_discoverTopology();

        print "<pre>";
        var_dump($row);
        debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
        print "</pre>";
*/
        parent::postConnect($writable);
    }
}