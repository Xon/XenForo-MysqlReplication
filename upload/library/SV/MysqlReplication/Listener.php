<?php

/**
 * Class SV_MysqlReplication_Listener
 */
class SV_MysqlReplication_Listener
{
    /**
     * @param XenForo_Dependencies_Abstract $dependencies
     * @param array                         $data
     */
    public static function init_dependencies(XenForo_Dependencies_Abstract $dependencies, array $data)
    {
        // leave to allow easy upgrade
    }
}