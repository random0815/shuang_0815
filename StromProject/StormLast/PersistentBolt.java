package StormLast;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shuangmm on 2018/1/23
 */
public class PersistentBolt {
    private static Map<String,Object> hikariConfigMap = new HashMap<String, Object>(){{
        put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        put("dataSource.url", "jdbc:mysql://172.17.11.183:3306/ExceptionMonitor");
        put("dataSource.user","root");
        put("dataSource.password", "root");
    }};
    public static ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

    public static JdbcInsertBolt getJdbcInsertBolt(){

        List<Column> schemaColumns = Lists.newArrayList(new Column("fan_no", Types.VARCHAR),
                new Column("call_time", Types.VARCHAR), new Column("call_count", Types.INTEGER));
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(schemaColumns);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider,simpleJdbcMapper)
                .withInsertQuery("insert into exception_output(fan_no,call_time,call_count) values(?,?,?)")
                .withQueryTimeoutSecs(50);
        return jdbcInsertBolt;
        //使用tablename进行插入数据，需要指定表中的所有字段
        /*String tableName="ExceptionOutput";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                                        .withTableName("ExceptionOutput")
                                        .withQueryTimeoutSecs(50);*/
        //使用schemaColumns，可以指定字段要插入的字段
    }

   /* public static JdbcLookupBolt getJdbcLookupBlot(){
        //查询
        //指定bolt的输出字段
        Fields outputFields = new Fields("user_id","resource_id","count");
        //指定查询条件字段
        List<Column> queryColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR),new Column("resource_id",Types.VARCHAR));
        String selectSql = "select count from userinfo where user_id=? and resource_id=?";
        SimpleJdbcLookupMapper lookupMapper = new SimpleJdbcLookupMapper(outputFields, queryColumns);
        JdbcLookupBolt jdbcLookupBolt  = new JdbcLookupBolt(connectionProvider, selectSql, lookupMapper);
        return jdbcLookupBolt;
    }*/
}
