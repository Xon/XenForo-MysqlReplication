<?php

class SV_MysqlReplication_Masterslave extends Zend_Db_Adapter_Mysqli
{
    /** @var bool */
    protected $_usingMaster         = false;
    /** @var bool */
    protected $_connectedMaster     = false;
    /** @var bool */
    protected $_connectedSlaveId    = false;
    /** @var bool */
    protected $readOnlyTransaction  = false;
    /** @var bool */
    protected $_setTransactionLevel = false;

    /** @var array|null */
    protected $_master_config               = null;
    /** @var array|null */
    protected $_slave_config                = null;
    /** @var bool */
    protected $_setStrictMode               = true;
    /** @var string[] */
    protected $_attributesToCopy            = ['host', 'port', 'username', 'password', 'dbname', 'charset'];
    /** @var string|null */
    protected $_initialTransactionlevel     = null;
    /** @var string|null */
    protected $_transactionTransactionlevel = null;

    /**
     * SV_MysqlReplication_Masterslave constructor.
     *
     * @param $config
     * @throws Zend_Db_Adapter_Exception
     */
    public function __construct($config)
    {
        $xfConfig = XenForo_Application::getConfig();
        $charset = isset($xfConfig->db->charset) ? $xfConfig->db->charset : null;
        if ($charset !== null)
        {
            // XenForo forces the charset to utf8, allow it to be overrided
            $config['chareset'] = $charset;
        }

        parent::__construct($config);
        $this->_master_config = $config;
        $this->_setStrictMode = isset($xfConfig->db->strictMode) ? (boolean)$xfConfig->db->strictMode : true;
        $this->_slave_config = empty($xfConfig->db->slaves) ? [] : $xfConfig->db->slaves->toArray();
        if (!empty($xfConfig->db->master))
        {
            $this->_initialTransactionlevel = empty($xfConfig->db->master->initialTransactionlevel) ? null : $xfConfig->db->master->initialTransactionlevel;
            $this->_transactionTransactionlevel = empty($xfConfig->db->master->transactionTransactionlevel) ? null : $xfConfig->db->master->transactionTransactionlevel;
        }
        $this->_usingMaster = empty($this->_slave_config);
        foreach ($this->_slave_config as &$slave)
        {
            $this->copyAttributes($slave, $this->_master_config);
        }
    }

    /**
     * @param array $slave
     * @param array $master
     */
    public function copyAttributes(array &$slave, array $master)
    {
        foreach ($this->_attributesToCopy as $attribute)
        {
            if (!isset($slave[$attribute]) && isset($master[$attribute]))
            {
                $slave[$attribute] = $master[$attribute];
            }
        }
    }

    /**
     * @return Zend_Db_Adapter_Abstract
     * @throws Zend_Db_Adapter_Mysqli_Exception
     */
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
            $this->_connection->query("SET SESSION TRANSACTION ISOLATION LEVEL " . $this->_transactionTransactionlevel);
        }
        return parent::beginTransaction();
    }

    public function closeConnection()
    {
        $this->_setTransactionLevel = false;
        if ($this->isConnected() && $this->readOnlyTransaction)
        {
            $this->readOnlyTransaction = false;
            $this->_connection->query("COMMIT");
        }
        parent::closeConnection();
    }

    /**
     * @return bool
     */
    protected function _connectMasterSetup()
    {
        $this->_connectedSlaveId = false;
        $this->_config = $this->_master_config;
        $this->_connectedMaster = true;
        $this->closeConnection();

        return true;
    }

    /**
     * @param int|null $slaveId
     * @return bool
     */
    protected function _connectSlaveSetup($slaveId = null)
    {
        if ($slaveId === null)
        {
            $count = count($this->_slave_config);
            $slaveId = ($count > 1) ? mt_rand(0, $count - 1) : 0;
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

    /**
     * @throws Zend_Db_Adapter_Mysqli_Exception
     */
    protected function _rawConnect()
    {
        parent::_connect();
    }

    /**
     * @throws Zend_Db_Adapter_Mysqli_Exception
     */
    protected function _connect()
    {
        if ($this->_usingMaster && ($this->_connectedMaster === false || $this->_connection === null))
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
        if ($this->_connection)
        {
            return;
        }

        parent::_connect();
        if ($this->_connection && $newConnection)
        {
            $this->postConnect($writable);
        }
    }

    /**
     * @param bool $writable
     */
    public function postConnect($writable)
    {
        if ($this->_setStrictMode)
        {
            $this->_connection->query("SET @@session.sql_mode='STRICT_ALL_TABLES'");
        }
        if ($this->_initialTransactionlevel)
        {
            $this->_connection->query("SET SESSION TRANSACTION ISOLATION LEVEL " . $this->_initialTransactionlevel);
        }
        if (!$writable && $this->_connectedSlaveId !== false)
        {
            // use a readonly transaction to ensure writes fail against the slave
            $this->_connection->query("START TRANSACTION READ ONLY");
            $this->readOnlyTransaction = true;
        }
    }

    /** @var bool */
    protected $_skipMasterCheck = false;

    /**
     * @param string $sql
     * @param array $bind
     * @return bool
     */
    public function checkForWrites($sql, $bind)
    {
        if (stripos($sql, 'select') === 0 && stripos($sql, 'for update') === false && stripos($sql, 'is_used_lock') === false && stripos($sql, 'get_lock') === false || stripos($sql, 'explain') === 0)
        {
            return false;
        }

        return true;
    }

    /**
     * @param string|Zend_Db_Select $sql
     * @param array                 $bind
     * @return Zend_Db_Statement_Interface
     */
    protected function _masterQuery($sql, $bind = [])
    {
        return parent::query($sql, $bind);
    }

    /**
     * @param int                   $slaveId
     * @param string|Zend_Db_Select $sql
     * @param array                 $bind
     * @return Zend_Db_Statement_Interface
     */
    protected function _slaveQuery($slaveId, $sql, $bind = [])
    {
        return parent::query($sql, $bind);
    }

    /**
     * @param string|Zend_Db_Select $sql
     * @param array $bind
     * @return Zend_Db_Statement_Interface
     * @throws Zend_Db_Adapter_Mysqli_Exception
     */
    public function query($sql, $bind = [])
    {
        if (!$this->_usingMaster && !$this->_skipMasterCheck)
        {
            if ($sql instanceof Zend_Db_Select)
            {
                if (empty($bind))
                {
                    $bind = $sql->getBind();
                }
                $sql = ltrim($sql->assemble());
            }
            else
            {
                $sql = ltrim($sql);
            }

            if ($this->checkForWrites($sql, $bind))
            {
                $this->_usingMaster = true;
                $this->_connect();
            }
        }
        else if (is_string($sql))
        {
            $sql = ltrim($sql);
        }
        if ($this->_connection === null)
        {
            $this->_connect();
        }

        try
        {
            return $this->_usingMaster ? $this->_masterQuery($sql, $bind) : $this->_slaveQuery($this->_connectedSlaveId, $sql, $bind);
        }
        catch (Zend_Db_Adapter_Mysqli_Exception $e)
        {
            // IF this is a readonly connection AND caused by a list of known safe errors; try again once.
            if ($this->readOnlyTransaction && $this->causedByLostConnection($e))
            {
                return $this->_usingMaster ? $this->_masterQuery($sql, $bind) : $this->_slaveQuery($this->_connectedSlaveId, $sql, $bind);
            }
            throw $e;
        }
    }

    /** @var string[] */
    static $serverGoneAwayMessages = [
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
    ];

    /**
     * @param Exception $e
     * @return bool
     */
    protected function causedByLostConnection(Exception $e)
    {
        $message = utf8_strtolower($e->getMessage());
        foreach (self::$serverGoneAwayMessages as $ErrorMessage)
        {
            if (utf8_strpos($message, $ErrorMessage) !== false)
            {
                return true;
            }
        }

        return false;
    }
}
