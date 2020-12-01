package com.zhbr.cqljgt.demo.hive;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @ClassName HiveJdbcConfig
 * @Description TODO
 * @Autor yanni
 * @Date 2020/11/21 11:11
 * @Version 1.0
 **/
@Configuration
public class HiveJdbcConfig {

    //@Value("${hive.url}")
    private String url ="jdbc:hive2://";

    //@Value("${hive.driver-class-name}")
    private String driver ="org.apache.hive.jdbc.HiveDriver";

    //@Value("${hive.user}")
    private String user = "cq_sjzt_sjml";

    //@Value("${hive.password}")
    private String password = "sjml_2020!";

    @Bean
    public DataSource dataSource() throws IOException {

        DataSource dataSource = new DataSource();
        StringBuilder sBuilder = new StringBuilder();
        //String CONF_DIR = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String CONF_DIR = "/var/cqljgt_springboot/conf/";
        String HIVE_CLIENT_PROPERTIES = "hiveclient.properties";
        ClientInfo clientInfo = new ClientInfo(CONF_DIR + HIVE_CLIENT_PROPERTIES);

        Boolean isSecurityMode = "KERBEROS".equalsIgnoreCase(clientInfo.getAuth());

        if (isSecurityMode) {
            sBuilder.append("jdbc:hive2://").append(clientInfo.getZkQuorum()).append("/").append(";serviceDiscoveryMode=")
                    .append(clientInfo.getServiceDiscoveryMode())
                    .append(";zooKeeperNamespace=")
                    .append(clientInfo.getZooKeeperNamespace())
                    .append(";sasl.qop=")
                    .append(clientInfo.getSaslQop())
                    .append(";auth=")
                    .append(clientInfo.getAuth())
                    .append(";principal=")
                    .append(clientInfo.getPrincipal())
                    .append(";");
        } else {
            sBuilder.append("jdbc:hive2://").append(clientInfo.getZkQuorum()).append("/").append(";serviceDiscoveryMode=")
                    .append(clientInfo.getServiceDiscoveryMode())
                    .append(";zooKeeperNamespace=")
                    .append(clientInfo.getZooKeeperNamespace())
                    .append(";auth=none");
        }

        dataSource.setUrl(sBuilder.toString());
        dataSource.setDriverClassName(driver);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    public static class ClientInfo {
        //The zk quorum info, format like: ip1:port,ip2:port...
        private String zkQuorum = null;
        private String auth = null;
        private String saslQop = null;
        private String zooKeeperNamespace = null;
        private String serviceDiscoveryMode = null;
        private String principal = null;

        private Properties clientInfo = null;

        public ClientInfo(String hiveclientFile) throws IOException {
            InputStream fileInputStream = null;
            try {
                clientInfo = new Properties();
                File propertiesFile = new File(hiveclientFile);
                fileInputStream = new FileInputStream(propertiesFile);
                clientInfo.load(fileInputStream);
            } catch (Exception e) {
                throw new IOException(e);
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                    fileInputStream = null;
                }
            }
            initialize();
        }

        private void initialize() {
            zkQuorum = clientInfo.getProperty("zk.quorum");
            auth = clientInfo.getProperty("auth");
            saslQop = clientInfo.getProperty("sasl.qop");
            zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
            serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
            principal = clientInfo.getProperty("principal");
        }

        public String getZkQuorum() {
            return zkQuorum;
        }

        public String getSaslQop() {
            return saslQop;
        }

        public String getAuth() {
            return auth;
        }

        public String getZooKeeperNamespace() {
            return zooKeeperNamespace;
        }

        public String getServiceDiscoveryMode() {
            return serviceDiscoveryMode;
        }

        public String getPrincipal() {
            return principal;
        }
    }
}
