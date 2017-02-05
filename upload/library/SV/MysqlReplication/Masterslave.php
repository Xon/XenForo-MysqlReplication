<?php

class SV_MysqlReplication_Masterslave extends Zend_Db_Adapter_Mysqli
{
    protected $_usingMaster = false;
    protected $_connectedMaster = false;
    protected $_connectedSlaveId = null;
    protected $readOnlyTransaction = false;

    protected $_master_config = null;
    protected $_slave_config = null;
    protected $_setStrictMode = true;
    protected $_attributesToCopy = array('host', 'port', 'username', 'password', 'dbname', 'charset');

    public function __construct($config)
    {
        parent::__construct($config);
        $this->_master_config = $config;
        $xfconfig = XenForo_Application::getConfig();
        $this->_setStrictMode = isset($xfconfig->db->strictMode) ? true : (boolean)$xfconfig->db->strictMode;
        $this->_slave_config = empty($xfconfig->db->slaves) ? array() : $xfconfig->db->slaves->toArray();
        $this->_usingMaster = empty($this->_slave_config);
        foreach($this->_slave_config as &$slave)
        {
            $this->copyAttributes($slave, $this->_master_config);
        }
    }

    public function copyAttributes(array &$slave, array $master)
    {
        foreach($this->_attributesToCopy as $attribute)
        {
            if (!isset($slave[$attribute]) && isset($master[$attribute]))
            {
                $slave[$attribute] = $master[$attribute];
            }
        }
    }

    public function beginTransaction()
    {
        $this->_usingMaster = true;
        parent::beginTransaction();
    }

    public function closeConnection()
    {
        if ($this->isConnected() && $this->readOnlyTransaction) {
            $this->readOnlyTransaction = false;
            $this->_connection->query("COMMIT");
        }
        parent::closeConnection();
    }

    protected function _connectMasterSetup()
    {
        $this->_connectedSlaveId = null;
        $this->_config = $this->_master_config;
        $this->_connectedMaster = true;
        $this->closeConnection();

        return true;
    }

    protected function _connectSlaveSetup($slaveId = null)
    {
        if ($slaveId === null)
        {
            $count = count($this->_slave_config);
            $slaveId =($count > 1) ? mt_rand(0,$count-1) : 0;
        }
        if ($slaveId === $this->_connectedSlaveId)
        {
            return false;
        }

        $this->_connectedSlaveId = $slaveId;
        $this->_config = $this->_slave_config[$this->_connectedSlaveId];

        $this->_connectedMaster = false;
        $this->closeConnection();

        return true;
    }

    protected function _connect()
    {
        if ($this->_usingMaster && ($this->_connectedMaster !== null  || $this->_connection == null))
        {
            $newConnection = $this->_connectMasterSetup();
            $writable = true;
        }
        else if (!$this->_usingMaster && ($this->_connectedSlaveId !== null || $this->_connection == null))
        {
            $newConnection = $this->_connectSlaveSetup();
            $writable = false;
        }
        else
        {
            $newConnection = false;
            $writable = false;
        }
        parent::_connect();
        if ($newConnection)
        {
            $this->postConnect($writable);
        }
    }

    public function postConnect($writable)
    {
        if ($this->_setStrictMode)
        {
            $this->_connection->query("SET @@session.sql_mode='STRICT_ALL_TABLES'");
        }
        if (!$writable && $this->_connectedSlaveId)
        {
            // use a readonly transaction to ensure writes fail against the slave
            $this->_connection->query("START TRANSACTION READ ONLY");
            $this->readOnlyTransaction = true;
        }
    }

    protected $_skipMasterCheck = false;

    public function checkForWrites($sql, $bind)
    {
        if (stripos($sql,'select') === 0 && stripos($sql, 'for update') === false || stripos($sql,'explain') === 0)
        {
            return false;
        }
        return true;
    }

    public function query($sql, $bind = array())
    {
        if (!$this->_usingMaster && !$this->_skipMasterCheck)
        {
            if ($sql instanceof Zend_Db_Select)
            {
                if (empty($bind))
                {
                    $bind = $sql->getBind();
                }
                $sql_string = ltrim($sql->assemble());
            }
            else
            {
                $sql_string = ltrim($sql);
            }

            if ($this->checkForWrites($sql_string, $bind))
            {
                $this->_usingMaster = true;
            }
        }

        return parent::query($sql, $bind);
    }
}