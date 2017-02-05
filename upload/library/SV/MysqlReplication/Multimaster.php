<?php

class SV_MysqlReplication_Multimaster extends SV_MysqlReplication_Masterslave
{
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
}