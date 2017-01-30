<?php

class SV_MysqlReplication_Masterslave extends Zend_Db_Adapter_Mysqli
{
    protected $_usingMaster = false;
    protected $_connectedMaster = false;
    protected $_connectedSlaveId = null;

    protected $_master_config = null;
    protected $_slave_config = null;
    protected $_setStrictMode = true;

    public function __construct($config)
    {
        parent::__construct($config);
        $this->_master_config = $config;
        $xfconfig = XenForo_Application::getConfig();
        $this->_setStrictMode = isset($xfconfig->db->strictMode) ? true : (boolean)$xfconfig->db->strictMode;
        $this->_slave_config = empty($xfconfig->db->slaves) ? array() : $xfconfig->db->slaves->toArray();
        $this->_usingMaster = empty($this->_slave_config);
    }

    public function beginTransaction()
    {
        $this->_usingMaster = true;
        parent::beginTransaction();
    }

    protected function _connect()
    {
        if ($this->_usingMaster && (!$this->_connectedMaster || $this->_connection == null))
        {
            $this->_connectedSlaveId = null;
            $this->_config = $this->_master_config;
            $this->_connectedMaster = true;
            $this->closeConnection();
            $newConnection = true;
        }
        else if (!$this->_usingMaster && ($this->_connectedMaster || $this->_connection == null))
        {
            $count = count($this->_slave_config);
            $this->_connectedSlaveId = ($count > 1) ? mt_rand(0,$count-1) : 0;
            $this->_config = $this->_slave_config[$this->_connectedSlaveId];

            $this->_connectedMaster = false;
            $this->closeConnection();
            $newConnection = true;
        }
        else
        {
            $newConnection = false;
        }
        parent::_connect();
        if ($newConnection)
        {
            $this->postConnect();
        }
    }

    public function postConnect()
    {
        if (!empty($this->_config['charset']))
        {
            mysqli_set_charset($this->_connection, $this->_config['charset']);
        }
        if ($this->_setStrictMode)
        {
            $this->_connection->query("SET @@session.sql_mode='STRICT_ALL_TABLES'");
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
                if ($this->_usingMaster) {
                    print "<pre>";
                    print $sql_string;
                    debug_print_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
                    print "</pre>";
                }
            }
        }

        return parent::query($sql, $bind);
    }
}