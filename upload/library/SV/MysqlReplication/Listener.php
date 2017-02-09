<?php

class SV_MysqlReplication_Listener
{
    public static function init_dependencies(XenForo_Dependencies_Abstract $dependencies, array $data)
    {
        if ($dependencies instanceof XenForo_Dependencies_Public)
        {
            $public_max_statement_time = XenForo_Application::getConfig()->db->public_max_statement_time;
            $db = XenForo_Application::getDb();
            if (is_callable(array($db, 'setStatementTimeout')) && $public_max_statement_time > 0)
            {
                $db->setStatementTimeout($public_max_statement_time);
            }   
        }
    }
}