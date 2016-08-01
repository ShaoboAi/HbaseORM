package com.pingnotes.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by shaobo on 8/1/16.
 */
public class HbaseConfig {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseConfig.class);

    private static final String ADDR = "hbase.zookeeper.quorum";
    private static final String PORT = "hbase.zookeeper.property.clientPort";
    private static Configuration conf;
    static {
        Properties prop = new Properties();
        InputStream is = HbaseDaoSupport.class.getClassLoader().getResourceAsStream("hbase.properties");
        try {
            prop.load(is);
            conf = HBaseConfiguration.create();
            conf.set(ADDR, prop.getProperty(ADDR));
            conf.set(PORT, prop.getProperty(PORT));
        } catch (IOException e) {
            LOG.error("load config error", e);
            System.exit(-1);
        }
    }

    public static final Configuration getConfig(){
        return conf;
    }
}
