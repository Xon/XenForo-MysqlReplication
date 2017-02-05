<?php

class SV_MysqlReplication_Multimaster extends SV_MysqlReplication_Masterslave
{
    protected $topology = null;
    protected $topologyDiscoverying = false;
    protected $prefix = null;

    public function __construct($config)
    {
        parent::__construct($config);
        $this->prefix = empty($config['galera']['apcu_prefix']) ? '' : $config['galera']['apcu_prefix'];
        $this->discovery = empty($config['galera']['discovery']) ? 5 : max(intval($config['galera']['discovery']), 1);
    }

    protected function _discoverTopology()
    {
        if ($this->topology !== null || $this->topologyDiscoverying)
        {
            return;
        }
        $this->topologyDiscoverying = true;
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
            $useTopology = false;
            $lastError = null;
            do
            {
                // remove this host from consideration
                if (empty($this->_master_config['invalid']) && $this->_master_config['host'] == $this->config['host'] && $this->_master_config['post'] == $this->config['post'])
                {
                    $this->_master_config['invalid'] = true;
                    $this->_connectedSlaveId = null;
                    $this->_config = $this->_master_config;
                    $this->_connectedMaster = true;
                    $this->closeConnection();
                }
                foreach($this->_slave_config as $key => &$connection)
                {
                    if ($connection['host'] == $this->config['host'] && $connection['post'] == $this->config['post'] )
                    {
                        unset($this->_slave_config[$key]);
                    }
                }
                // pick a random surviving slave.  This is racy, but that is ok.
                if ($this->_connection === null)
                {
                    $count = count($this->_slave_config);
                    $this->_connectedSlaveId = ($count > 1) ? mt_rand(0,$count-1) : 0;
                    $this->_config = $this->_slave_config[$this->_connectedSlaveId];
                    $this->_connectedMaster = false;
                    $this->closeConnection();
                }
                try
                {
                    $this->_rawConnect();
                }
                catch(Zend_Db_Statement_Mysqli_Exception $e)
                {
                    $this->closeConnection();
                    $lastError = $e;
                    // https://dev.mysql.com/doc/refman/5.6/en/error-messages-client.html
                    // https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
                    if (mysqli_connect_error() == 0)
                    {
                       throw $e;
                    }
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
                $this->closeConnection();
            }
            //TODO: need to fix this loop condition
            while ( $this->_connection !== null || $this->_slave_config );

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

            //$this->_connectedSlaveId
            //$this->_connectedMaster
            //$this->_usingMaster
        }

        return;
    }

    protected function _connectMasterSetup()
    {
        if ($this->topologyDiscoverying || !parent::_connectMasterSetup())
        {
            return false;
        }
        try
        {
            $this->_discoverTopology();
        }
        finally
        {
            $this->topologyDiscoverying = false;
        }
        return $this->_connection === null;
    }

    protected function _connectSlaveSetup($slaveId = null)
    {
        if ($this->topologyDiscoverying || !parent::_connectSlaveSetup($slaveId))
        {
            return false;
        }
        try
        {
            $this->_discoverTopology();
        }
        finally
        {
            $this->topologyDiscoverying = false;
        }

        return $this->_connection === null;
    }

    protected $lastQueryReadonly = false;

    public function checkForWrites($sql, $bind)
    {
        $this->lastQueryReadonly = parent::checkForWrites($sql, $bind);
        return $this->lastQueryReadonly;
    }

    public function query($sql, $bind = array())
    {
        $this->lastQueryReadonly = false;
        try
        {
            return parent::query($sql, $bind);
        }
        catch(Zend_Db_Statement_Mysqli_Exception $e)
        {
            // https://dev.mysql.com/doc/refman/5.6/en/error-messages-client.html
            // https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
            // mark the server as failed depending on the errror
            $lastError = mysqli_errno();
            switch($lastError)
            {
                // client-side errors that we can ignore
                case 2058: // This handle is already connected. Use a separate handle for each connection.
                case 2060: // There is an attribute with the same name already
                case 2057: // The number of columns in the result set differs from the number of bound buffers. You must reset the statement, rebind the result set columns, and execute the statement again
                case 2056: // Statement closed indirectly because of a preceeding %s() call
                case 2053: // Attempt to read a row while there is no result set associated with the statement
                case 2052: // Prepared statement contains no metadata
                case 2051: // Attempt to read column without prior row fetch
                case 2050: // Row retrieval was canceled by mysql_stmt_close() call
                case 2036: // Using unsupported buffer type: %d (parameter: %d)
                case 2035: // Can't send long data for non-string/non-binary data types (parameter: %d)
                case 2034: // Invalid parameter number
                case 2033: // No parameters exist in the statement
                case 2032: // Data truncated
                case 2031: // No data supplied for parameters in prepared statement
                case 2030: // Statement not prepared
                case 2023: // Error on SHOW SLAVE HOSTS
                case 2022: // Error on SHOW SLAVE STATUS
                    break;
                // force a reconnection
                case 1053: // Server shutdown in progress
                case 1077: // %s: Normal shutdown
                case 1040: // Too many connections
                case 2055; // Lost connection to MySQL server at '%s', system error: %d
                case 2013: // Lost connection to MySQL server during query
                case 2006: // MySQL server has gone away
                    if ($this->lastQueryReadonly)
                    {
                        // for some errors on a known readonly query/connection, we can retry on a different server
                        $this->closeConnection();
                        // bad server, mark as dead

                        // retry
                        return parent::query($sql, $bind);
                    }
                    break;
                default:
                    if ($lastError > 2000)
                    {
                        // bad server (from client message), mark as dead
                    }
                    break;
            }

            throw $e;
        }
    }
}