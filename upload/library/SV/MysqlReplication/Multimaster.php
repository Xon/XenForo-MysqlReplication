<?php

class SV_MysqlReplication_Multimaster extends SV_MysqlReplication_Masterslave
{
    /**
     * @param $writable
     *
     * @return bool
     */
    protected function _doHealthCheck($writable)
    {
        /** @var \mysqli $connection */
        $connection = $this->_connection;

        $result = $connection->query("
          select VARIABLE_NAME, VARIABLE_VALUE
          from information_schema.GLOBAL_STATUS 
          where GLOBAL_STATUS.VARIABLE_NAME in ('WSREP_PROVIDER_NAME', 'WSREP_READY','WSREP_CONNECTED')
        ");
        if ($result === false || $result === true)
        {
            // isn't a slave
            return true;
        }
        /** @var \mysqli_result $result */
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        if (empty($rows) || !is_array($rows) || count($rows) < 2)
        {
            return false;
        }
        $isValid = true;
        foreach ($rows as $row)
        {
            switch($row['VARIABLE_NAME'])
            {
                case 'WSREP_PROVIDER_NAME':
                    if (empty($row['VARIABLE_VALUE']))
                    {
                        // not a Galera node
                        return true;
                    }
                    break;
                case 'WSREP_READY':
                case 'WSREP_CONNECTED':
                    if ($row['VARIABLE_VALUE'] !== 'ON')
                    {
                        $isValid = false;
                    }
                    break;
            }
        }
        return $isValid;
    }

    /**
     * @param string $sql
     * @param array  $bind
     * @return bool
     * @throws Zend_Db_Adapter_Mysqli_Exception
     */
/*
    public function checkForWrites($sql, $bind)
    {
        if (!parent::checkForWrites($sql, $bind))
        {
            return false;
        }

        // for multi-master allow session activity & session writes to hit the "slaves"
        if (substr($sql, 0, 31) === 'INSERT INTO xf_session_activity')
        {
            // bucket to one of the slaves, and then reconnect to the right slave to send writes to
            $hashKey = crc32(
                '' .
                $bind[0] . // user_id
                //$bind[1]   // unique_key
                $bind[2]   // ip
            );
            $hashIndex = floor($hashKey % count($this->_slave_config));
            if ($this->_connectSlaveSetup($hashIndex))
            {
                $this->_usingMaster = true;
                $this->_connectedMaster = true;
                $this->_rawConnect();
                $this->postConnect(true);
            }

            return false;
        }

        return true;
    }

*/
}
