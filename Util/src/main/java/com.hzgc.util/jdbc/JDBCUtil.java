package com.hzgc.util.jdbc;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.hzgc.util.FileUtil;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Melody on 2017-10-27.
 */
public class JDBCUtil {

    private static JDBCUtil instance = null;
    private static Logger log = Logger.getLogger(JDBCUtil.class);
    private static DataSource dataSource;
    private static Connection conn;
    private static Properties propertie = new Properties();
    private static File resourceFile;
    private static PreparedStatement pstmt;
    private static ResultSet rs;

    private JDBCUtil() {
    }

    /**
     * 加载数据源配置信息
     *
     */
    static {
        try {
            resourceFile = FileUtil.loadResourceFile("dbcp.properties");
            propertie.list(System.out);
            if (resourceFile != null) {
                propertie.load(new FileInputStream(resourceFile));
                System.out.println(propertie);
            }
            dataSource = DruidDataSourceFactory.createDataSource(propertie); //DruidDataSrouce工厂模式
            System.out.println(dataSource + " " + propertie);
        } catch (Exception e) {
            log.info("获取配置失败");
        }
    }

    /**
     * 获取单例
     *
     * @return 返回JDBCUtil单例对象
     */
    public static JDBCUtil getInstance() {
        if (instance == null) {
            synchronized (JDBCUtil.class) {
                if (instance == null) {
                    instance = new JDBCUtil();
                }
            }
        }
        return instance;
    }

    /**
     * 获取数据库连接池连接
     *
     * @return 返回Connection对象
     */
    public Connection getConnection() {
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params,
                             QueryCallback callback) {
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null || pstmt != null || rs != null) {
                try {
                    conn.close();
                    pstmt.close();

                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 静态内部类：查询回调接口
     *
     * @author Administrator
     */
    public interface QueryCallback {

        /**
         * 处理查询结果
         *
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }
}
