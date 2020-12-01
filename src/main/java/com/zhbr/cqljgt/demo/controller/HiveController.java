package com.zhbr.cqljgt.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhbr.cqljgt.demo.hive.ConnHuaweiHive;
import com.zhbr.cqljgt.demo.hive.HiveJdbcConfig;
import com.zhbr.cqljgt.demo.util.LoginUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

/**
 * @ClassName HiveController
 * @Description TODO
 * @Autor yanni
 * @Date 2020/11/21 9:55
 * @Version 1.0
 **/
@RestController
public class HiveController {

    private static Logger logger = LoggerFactory.getLogger(HiveController.class);
    private Integer num;
    private String realDB;

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @PostMapping("/sgcc/V1/query/sampledata")
    public JSONObject list(
            @RequestParam (value = "token") String token,
            @RequestParam (value = "appId") String appId,
            @RequestParam (value = "timestamp") String timestamp,
            @RequestParam (value = "systemCode") String systemCode,
            @RequestParam (value = "dbName") String dbName,
            @RequestParam(value = "tableName") String tableName) {

        JSONObject json  = new JSONObject();

        try {
            //UserGroupInformation.loginUserFromKeytab("zyn_1@F4F9624B_B718_496B_BF3D_D2AE94AD3005.COM", System.getProperty("hive_keytab"));
            init(true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //校验token
        boolean b = checkAuthorizeToken(token, appId, timestamp);

        if(b){
            logger.info("============ 校验token成功 ============");
            //读取映射文件
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(new FileReader("/var/cqljgt_springboot/ysb.json"));
            } catch (FileNotFoundException e) {
                logger.error("映射表读取失败："+e.getMessage());
            }

            String line = null;
            String message = new String();
            StringBuffer buffer = new StringBuffer();
            while (true) {
                try {
                    if (!((line = bufferedReader.readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // buffer.append(line);
                message += line;
            }

            JSONObject ysbStr = JSONObject.parseObject(message);
            JSONArray jsonArray = ysbStr.getJSONArray(systemCode);

            // 遍历jsons数组对象，
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                realDB = jsonObject.getString(dbName);
            }

            try {
                String setSQL = "set role admin";
                String schema = "desc " + realDB + "." + tableName;
                String select = "select * from " + realDB + "." + tableName+" limit 0,20";

                HiveJdbcConfig hiveJdbcConfig = new HiveJdbcConfig();
                DataSource dataSource = hiveJdbcConfig.dataSource();
                String newUrl = dataSource.getUrl();
                String username = dataSource.getUsername();
                String password = dataSource.getPassword();

                ConnHuaweiHive.set(newUrl,setSQL);
                List<Map<String, Object>> schemaList = ConnHuaweiHive.runDesc(newUrl, username, password, schema);
                List<Map<String, Object>> selectList = ConnHuaweiHive.runSelect(newUrl, username, password, select);

//                List<Map<String, Object>> schemaList = jdbcTemplate.queryForList(schema);
//                List<Map<String, Object>> selectList = jdbcTemplate.queryForList(select);

                num = selectList.size();

                if (selectList.size() > 0) {
                    String schemaJson = JSON.toJSONString(schemaList);
                    String selectJson = JSON.toJSONString(selectList);

                    StringBuffer stringBuffer = new StringBuffer();
                    stringBuffer.append("{\n" +
                        "    \"code\":0,\n" +
                        "    \"datas\":{\n" +
                        "        \"total\":" + num + ",\n" +
                        "        \"columns\":").append(schemaJson).append(",\"rows\":").append(selectJson).append("},\n" +
                        "    \"msg\":\"查询成功\"\n" +
                        "}");

                    logger.info("======== 表数据获取成功："+systemCode+" - "+realDB+"."+tableName+" =========");
                    json = JSONObject.parseObject(stringBuffer.toString());
                }
            }catch (Exception e){
                String error = "{\"msg\":\""+e.getMessage()+"\",\"code\":-1}";
                json = JSONObject.parseObject(error);
                logger.error(e.getMessage());
            }
        }
        return json;
    }

    /**
     *生成token
     * @param appId
     * @param appKey
     * @param timestamp
     * @return
     * @throws Exception
     */
    public String createToken(String appId, String appKey, String timestamp) throws Exception {
        try {
            //用MD5加密生成秘钥串
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes  = md.digest((appId+appKey+timestamp).getBytes("UTF-8"));
            BigInteger bigInt = new BigInteger(1, md5Bytes);//1代表绝对值
            String token = bigInt.toString(16);
            return token;
        } catch (Exception e) {
            logger.error("token创建异常", e);
            throw e;
        }
    }


    private boolean checkAuthorizeToken(String token, String appId, String timestamp) {
        try {
//            HttpServletRequest req = (HttpServletRequest) request;
//
//            String token = req.getParameter ("token");
//            String appId = req.getParameter ("appId");
//            String timestamp = req.getParameter ("timestamp");
            //获取本地保存的省公司秘钥
            String appKey = getAppkey(appId);

            //用MD5加密生成秘钥串
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5Bytes  = md.digest((appId+appKey+timestamp).getBytes("UTF-8"));
            BigInteger bigInt = new BigInteger(1, md5Bytes);//1代表绝对值
            String checkToken = bigInt.toString(16);

            if(!StringUtils.equals(token, checkToken)) {
                return false;
            }
            return true;
        } catch (Exception ex) {
            logger.error("token校验异常", ex);
            throw new RuntimeException("token校验异常");
        }
    }

    /**
     * 获取appKey
     * @param appId
     * @return
     */
    private String getAppkey(String appId){
        String appKey = "";
        if (appId.equals("01000")){
            appKey = "cde18afb42556eeed09783b437eece17";
        }
        return appKey;
    }

    private void init(boolean isSecurityMode) throws IOException {
        //String uuid = UUID.randomUUID().toString().replaceAll("-","");

        System.setProperty("java.security.krb5.conf","/var/cqljgt_springboot/conf/krb5.conf");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        if (isSecurityMode) {
            String userName = "cq_sjzt_sjml";
            String userKeytabFile = "/var/cqljgt_springboot/conf/user.keytab";
            String krb5File = "/var/cqljgt_springboot/conf/krb5.conf";
            conf.set(HADOOP_SECURITY_AUTHENTICATION, "Kerberos");
            conf.set(HADOOP_SECURITY_AUTHORIZATION, "true");
            conf.addResource(new FileInputStream(new File("/var/cqljgt_springboot/conf/core-site.xml")));
            conf.addResource(new FileInputStream(new File("/var/cqljgt_springboot/conf/hivemetastore-site.xml")));
            conf.addResource(new FileInputStream(new File("/var/cqljgt_springboot/conf/hive-site.xml")));

            LoginUtil.setJaasConf("Client", userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop");

            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }
}


