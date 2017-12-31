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
    /** @var int */
    protected $_healthCheckTTL = 0;

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
            $config['charset'] = $charset;
        }

        parent::__construct($config);
        $this->_master_config = $config;
        $this->_healthCheckTTL = isset($xfConfig->db->healthCheckTTL) ? (int)$xfConfig->db->healthCheckTTL : 0;
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
        $this->_connectedMaster = false;
        $this->_connectedSlaveId = false;
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
        $this->closeConnection();
        $this->_config = $this->_master_config;

        return true;
    }

    /**
     * @param int|null $slaveId
     * @return bool
     */
    protected function _connectSlaveSetup($slaveId = null)
    {
        // no slaves left, use the master
        if (empty($this->_slave_config))
        {
            return $this->_connectMasterSetup();
        }
        if ($slaveId === null)
        {
            $count = count($this->_slave_config);
            $slaveId = ($count > 1) ? mt_rand(0, $count - 1) : 0;
        }
        if ($slaveId === $this->_connectedSlaveId)
        {
            return false;
        }

        $this->closeConnection();
        $this->_connectedSlaveId = $slaveId;
        $this->_config = $this->_slave_config[$this->_connectedSlaveId];


        return true;
    }

    /**
     * @throws Zend_Db_Adapter_Mysqli_Exception
     */
    protected function _rawConnect()
    {
        parent::_connect();
    }

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

        if (!$this->_tryConnect($e))
        {
            if ($this->_usingMaster)
            {
                throw $e;
            }
            else
            {
                // fall back to the master
                $newConnection = $this->_connectMasterSetup();
                $writable = true;
                parent::_connect();
            }
        }

        if ($this->_connection && $newConnection)
        {
            $this->_connectedMaster = $this->_usingMaster;
            while ($this->_connection === null || !$this->doHealthCheck($writable))
            {
                if ($writable || empty($this->_slave_config))
                {
                    $writable = true;
                    $this->_connectMasterSetup();
                    parent::_connect();
                    $this->_connectedMaster = true;
                }
                else
                {
                    $this->_connectSlaveSetup();
                    $this->_tryConnect();
                }
            }
            $this->postConnect($writable);
        }
    }

    /**
     * @param $writable
     *
     * @return bool
     */
    protected function _doHealthCheck($writable)
    {
        if ($writable)
        {
            return true;
        }

        /** @var \mysqli $connection */
        $connection = $this->_connection;

        $result = $connection->query("show slave status");
        if ($result === false || $result === true)
        {
            if (!$result && $connection->errno === 1227)
            {
                // Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
                // unknown if it is a slave or not, don't use it
                return false;
            }

            // isn't a slave
            return true;
        }
        /** @var \mysqli_result $result */
        $row = $result->fetch_assoc();
        if (isset($row['Seconds_Behind_Master']) && $row['Seconds_Behind_Master'] > 0)
        {
            // slave is behind, don't use it
            return  false;
        }
        if (isset($row['Slave_IO_Running']) && $row['Slave_IO_Running'] !== 'Yes')
        {
            // slave is not running, don't use it
            return  false;
        }
        if (isset($row['Slave_SQL_Running']) && $row['Slave_SQL_Running'] !== 'Yes')
        {
            // slave is not running, don't use it
            return false;
        }
        return !empty($row);
    }

    protected function doHealthCheck($writable)
    {
        // do not allow the master/writable connection to be disabled
        if ($writable)
        {
            return true;
        }

        if (empty($this->_config['replication_health_check']))
        {
            return true;
        }

        $cache = null;
        if ($this->_healthCheckTTL)
        {
            try
            {
                $cache = XenForo_Application::getCache();
            }
            catch (\Exception $e)
            {
            }
        }
        $credis = null; $backend = null; $key = null; $redisKey = null;
        if ($cache)
        {
            /** @var Zend_Cache_Backend_Redis $backend */
            $backend = $cache->getBackend();
            $key = "{$this->_config['host']}_{$this->_config['port']}_{$this->_config['username']}_{$this->_config['dbname']}";
            if (method_exists($backend, 'getCredis') && $credis = $backend->getCredis(true))
            {
                $redisKey = Cm_Cache_Backend_Redis::PREFIX_KEY . $cache->getOption('cache_id_prefix') . 'db.health.' . $key;
                $obj = $credis->get($redisKey);
            }
            else
            {
                $obj = $cache->load($key);
            }
            if ($obj !== false)
            {
                $isValid = $obj ? true : false;
                if (!$isValid && $this->_connectedSlaveId !== false)
                {
                    unset($this->_slave_config[$this->_connectedSlaveId]);
                    $this->_slave_config = array_values($this->_slave_config);
                    $this->closeConnection();
                }
                return $isValid;
            }
        }

        $isValid = $this->_doHealthCheck($writable);

        if (!$isValid && $this->_connectedSlaveId !== false)
        {
            unset($this->_slave_config[$this->_connectedSlaveId]);
            $this->_slave_config = array_values($this->_slave_config);
            $this->closeConnection();
        }

        if ($cache)
        {
            if (method_exists($backend, 'getCredis') && $credis = $backend->getCredis())
            {
                $credis->set($redisKey, $isValid ? '1' : '', $this->_healthCheckTTL);
            }
            else
            {
                $cache->save($isValid ? '1' : '', $key, [], $this->_healthCheckTTL);
            }
        }

        return $isValid;
    }

    /**
     * @param Exception|null $exception
     * @return bool
     */
    protected function _tryConnect(Exception &$exception = null)
    {
        try
        {
            parent::_connect();
        }
        catch (Zend_Db_Adapter_Mysqli_Exception $e)
        {
            if ($e->getMessage() === 'Connection refused')
            {
                if ($this->_connectedSlaveId !== false)
                {
                    unset($this->_slave_config[$this->_connectedSlaveId]);
                    $this->_slave_config = array_values($this->_slave_config);
                    $this->closeConnection();
                }

                $cache = null;
                if ($this->_healthCheckTTL)
                {
                    try
                    {
                        $cache = XenForo_Application::getCache();
                    }
                    catch (\Exception $e)
                    {
                    }
                }
                if ($cache)
                {
                    /** @var Zend_Cache_Backend_Redis $backend */
                    $backend = $cache->getBackend();
                    $key = "{$this->_config['host']}_{$this->_config['port']}_{$this->_config['username']}_{$this->_config['dbname']}";
                    if (method_exists($backend, 'getCredis') && $credis = $backend->getCredis())
                    {
                        $redisKey = Cm_Cache_Backend_Redis::PREFIX_KEY . $cache->getOption('cache_id_prefix') . 'db.health.' . $key;
                        $credis->set($redisKey, '', $this->_healthCheckTTL);
                    }
                    else
                    {
                        $cache->save('', $key, [], $this->_healthCheckTTL);
                    }
                }
            }
            $exception = $e;

            return false;
        }

        return true;
    }

    /**
     * @param bool $writable
     */
    public function postConnect($writable)
    {
        /** @var \mysqli $connection */
        $connection = $this->_connection;

        if ($this->_setStrictMode)
        {
            $connection->query("SET @@session.sql_mode='STRICT_ALL_TABLES'");
        }
        if ($this->_initialTransactionlevel)
        {
            $connection->query("SET SESSION TRANSACTION ISOLATION LEVEL " . $this->_initialTransactionlevel);
        }
        if (!$writable && $this->_connectedSlaveId !== false)
        {
            // use a readonly transaction to ensure writes fail against the slave
            $connection->query("START TRANSACTION READ ONLY");
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
