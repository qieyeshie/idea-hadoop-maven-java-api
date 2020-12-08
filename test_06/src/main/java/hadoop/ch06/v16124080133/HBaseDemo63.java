package hadoop.ch06.v16124080133;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo63 {
    
    /* create table. */
    public static void createTable(String tableName, String[] family) throws Exception {

        //创建到 HBase 的连接
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");

        Connection connect = ConnectionFactory.createConnection(conf);
        Admin admin = connect.getAdmin();

        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor desc = new HTableDescriptor(tn);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }

        if (admin.tableExists(tn)) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    /* put data */
    public static void insert(String rowKey, String tableName, String[] column1, String[] value1, String[] column2, String[] value2, String[] column3, String[] value3) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");

        Connection connect = ConnectionFactory.createConnection(conf);
        TableName tn = TableName.valueOf(tableName);
        Table table = connect.getTable(tn);

        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String f = columnFamilies[i].getNameAsString();
            if (f.equals("member_id")) {
                for (int j = 0; j < column1.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (f.equals("address")) {
                for (int j = 0; j < column2.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
            if (f.equals("info")) {
                for (int j = 0; j < column3.length; j++) {
                    put.addColumn(Bytes.toBytes(f), Bytes.toBytes(column3[j]), Bytes.toBytes(value3[j]));
                }
            }
        }
        table.put(put);
        System.out.println("insert data Success!");
    }

    /* get data. */
    public static void getResult(String rowKey, String colfamily, String column) throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");
        //指定客户端
        HTable table = new HTable(conf, "emp16124080133");

        //通过get()查询
        Get get = new Get(Bytes.toBytes(rowKey));

        Result record = table.get(get);

        String result = Bytes.toString(record.getValue(Bytes.toBytes(colfamily),Bytes.toBytes(column)));
        System.out.print("联合Rain, info, birthday, 查询结果：");
        System.out.println(result);
    }



    /* scan data. */
    public static void getResultByColumn() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");
        //指定客户端
        HTable table = new HTable(conf, "emp16124080133");

        //指定扫描器
        Scan scanner =new Scan();   //相当于select * from student

        ResultScanner rs = table.getScanner(scanner);

        for (Result r:rs) {
            String id =Bytes.toString(r.getValue(Bytes.toBytes("member_id"),Bytes.toBytes("id")));
            String age =Bytes.toString(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("age")));
            String birthday =Bytes.toString(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("birthday")));
            String industry =Bytes.toString(r.getValue(Bytes.toBytes("info"),Bytes.toBytes("industry")));
            String city =Bytes.toString(r.getValue(Bytes.toBytes("address"),Bytes.toBytes("city")));
            String country =Bytes.toString(r.getValue(Bytes.toBytes("address"),Bytes.toBytes("country")));
            System.out.println("查询结果："+"id:"+id+"     " +"age:"+age + "    " +"birthday:"+ birthday+ "     "+"industry:"+industry+ "     "+"city:"+city+ "     "+"country:"+country);
        }
    }

    public static void  main (String [] agrs) throws Exception {


        try {
            /* create table. */
            String tableName = "emp16124080133";
            String[] family = new String[]{"member_id", "address", "info"};
            createTable(tableName, family);

            /* insert data. */
            String[] column1 = {"id" };
            String[] value1 = {"31"};
            String[] column2 = {"city", "country" };
            String[] value2 = {"ShenZhen","Chinese"};
            String[] column3 = { "age", "birthday", "industry" };
            String[] value3 = {"28", "1990-05-01", "architect"};
            insert("Rain", tableName, column1, value1, column2, value2, column3, value3);
            insert("Rain", tableName, column1, value1, column2, value2, column3, value3);
            insert("Rain", tableName, column1, value1, column2, value2, column3, value3);

            /* get data. */
            getResult("Rain","info", "birthday");

            /* get data. */
            getResultByColumn();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}