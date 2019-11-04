package com.comment;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtils {
    /**
     * mysql配置
     * @return
     */
    public static Map<String,String> get_MYSQL_PROP(InputStream is){
        Map<String,String> map = new HashMap<>();
        try{
            Properties properties = new Properties();
            properties.load(is);
            map.put("jdbc.username", properties.getProperty(Conf.MYSQL_USRENAME));
            map.put("jdbc.pw", properties.getProperty(Conf.MYSQL_PW));
            map.put("jdbc.url", properties.getProperty(Conf.MYSQL_URL));
            map.put("driver","com.mysql.jdbc.Driver");
            return map;
        }catch (Exception e){
            e.printStackTrace();
        }
        return map;
    }
}
