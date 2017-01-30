<?php

class SV_MysqlReplication_Multimaster extends SV_MysqlReplication_Masterslave
{
    protected $topology = null;

    protected function _discoverTopology()
    {
        if ($this->topology !== null)
        {
            return;
        }
        $topology = false;
        if (function_exists('apcu_fetch'))
        {
            $prefix = empty($this->_master_config['galera']['apcu_prefix']) ? '' : $this->_master_config['galera']['apcu_prefix'];
            $topology = apcu_fetch($prefix . 'galera' );    
        }
        if ($topology === false)
        {
            
        }
        
        $this->topology = $topology;        
    }

    protected function _connectMasterSetup()
    {
        $this->_discoverTopology();
        return parent::_connectMasterSetup();
    }

    protected function _connectSlaveSetup($slaveId = null)
    {
        $this->_discoverTopology();
        return parent::_connectSlaveSetup($slaveId);
    }

    //SHOW GLOBAL STATUS LIKE 'wsrep_local_index';
    //SHOW GLOBAL STATUS LIKE 'wsrep_cluster_size';
    //SHOW GLOBAL STATUS LIKE 'wsrep_cluster_status'; == 'Primary'
    //SHOW GLOBAL STATUS LIKE 'wsrep_ready'; == 'ON'
    //wsrep_incoming_addresses
    public function postConnect($writable)
    {
        $this->_discoverTopology();
        // sanity check the connection. Use the information schema to save on round-trips
        $stmt = $this->_connection->query("
             select * from information_schema.GLOBAL_STATUS where VARIABLE_NAME in ('wsrep_local_index','wsrep_cluster_size','wsrep_cluster_status','wsrep_ready');
        ");
        $stmt->
        print "<pre>";
        //var_dump($rows);
        debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
        print "</pre>";
        parent::postConnect($writable);
    }
}