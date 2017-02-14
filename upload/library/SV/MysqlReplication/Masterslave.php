<?php

class SV_MysqlReplication_Masterslave extends Zend_Db_Adapter_Mysqli
{
    protected $_usingMaster = false;
    protected $_connectedMaster = false;
    protected $_connectedSlaveId = false;
    protected $readOnlyTransaction = false;
    protected $_max_statement_time = null;
    protected $_setTransactionLevel = false;

    protected $_master_config = null;
    protected $_slave_config = null;
    protected $_setStrictMode = true;
    protected $_attributesToCopy = array('host', 'port', 'username', 'password', 'dbname', 'charset');
    protected $_initialTransactionlevel = null;
    protected $_transactionTransactionlevel = null;

    public function __construct($config)
    {
        parent::__construct($config);
        $this->_master_config = $config;
        $xfconfig = XenForo_Application::getConfig();
        $this->_setStrictMode = isset($xfconfig->db->strictMode) ? true : (boolean)$xfconfig->db->strictMode;
        $this->_slave_config = empty($xfconfig->db->slaves) ? array() : $xfconfig->db->slaves->toArray();
        if (!empty($xfconfig->db->master))
        {
            $this->_initialTransactionlevel = empty($xfconfig->db->master->initialTransactionlevel) ? null : $xfconfig->db->master->initialTransactionlevel;
            $this->_transactionTransactionlevel = empty($xfconfig->db->master->transactionTransactionlevel) ? null : $xfconfig->db->master->transactionTransactionlevel;
        }
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
        if ($this->_transactionTransactionlevel && !$this->_setTransactionLevel)
        {
            $this->_setTransactionLevel = true;
            if (!$this->_connectedMaster)
            {
                $this->_connect();
            }
            $this->_connection->query("SET SESSION TRANSACTION ISOLATION LEVEL ". $this->_transactionTransactionlevel);
        }
        parent::beginTransaction();
    }

    public function closeConnection()
    {
        $this->_setTransactionLevel = false;
        if ($this->isConnected() && $this->readOnlyTransaction) {
            $this->readOnlyTransaction = false;
            $this->_connection->query("COMMIT");
        }
        parent::closeConnection();
    }

    protected function _connectMasterSetup()
    {
        $this->_connectedSlaveId = false;
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

    protected function _rawConnect()
    {
        parent::_connect();
    }

    protected function _connect()
    {
        if ($this->_usingMaster && ($this->_connectedMaster === false  || $this->_connection === null))
        {
            $newConnection = $this->_connectMasterSetup();
            $writable = true;
        }
        else if (!$this->_usingMaster && ($this->_connectedSlaveId === false || $this->_connection === null))
        {
            $newConnection = $this->_connectSlaveSetup();
            $writable = false;
        }
        else
        {
            $newConnection = false;
            $writable = false;
        }
        if ($this->_connection) {
            return;
        }

        parent::_connect();
        if ($this->_connection && $newConnection)
        {
            $this->postConnect($writable);
        }
    }

    public function setStatementTimeout($timeout)
    {
        $this->_max_statement_time = strval(floatval($timeout)) + 0;
        if ($this->_connection)
        {
            $this->_connection->query("SET @@session.max_statement_time=". $this->_max_statement_time);
        }
    }

    public function postConnect($writable)
    {
        if ($this->_max_statement_time)
        {
            $this->_connection->query("SET @@session.max_statement_time=". $this->_max_statement_time);
        }
        if ($this->_setStrictMode)
        {
            $this->_connection->query("SET @@session.sql_mode='STRICT_ALL_TABLES'");
        }
        if ($this->_initialTransactionlevel)
        {
            $this->_connection->query("SET SESSION TRANSACTION ISOLATION LEVEL ". $this->_initialTransactionlevel);
        }
        if (!$writable && $this->_connectedSlaveId !== false)
        {
            // use a readonly transaction to ensure writes fail against the slave
            $this->_connection->query("START TRANSACTION READ ONLY");
            $this->readOnlyTransaction = true;
        }
    }

    protected $_skipMasterCheck = false;

    public function checkForWrites($sql, $bind)
    {
        if (stripos($sql,'select') === 0 && stripos($sql, 'for update') === false && stripos($sql, 'is_used_lock') === false && stripos($sql, 'get_lock') === false  || stripos($sql,'explain') === 0)
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

        try
        {
            return parent::query($sql, $bind);
        }
        catch(Zend_Db_Adapter_Mysqli_Exception $e)
        {
            // IF this is a readonly connection AND caused by a list of known safe errors; try again once.
            if ($this->readOnlyTransaction && $this->causedByLostConnection($e))
            {
                return parent::query($sql, $bind);
            }
            throw $e;
        }
    }

    static $serverGoneAwayMessages = array(
        'server has gone away',
        'no connection to the server',
        'Lost connection',
        'is dead or not enabled',
        'error while sending',
        'decryption failed or bad record mac',
        'server closed the connection unexpectedly',
        'ssl connection has been closed unexpectedly',
        'deadlock found when trying to get lock',
        'error writing data to the connection',
        'resource deadlock avoided',
        'query execution was interrupted',
    );

    protected function causedByLostConnection(Exception $e)
    {
        $message = utf8_strtolower($e->getMessage());
        foreach(self::$serverGoneAwayMessages as $ErrorMessage)
        {
            if (utf8_strpos($message, $ErrorMessage) !== false)
            {
                return true;
            }
        }
        return false;
    }
}