package com.gennlife.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author liumingxin
 * @create 2018 09 19:43
 * @desc
 **/
public class HbaseUtils {

    private static final Logger LOG= LoggerFactory.getLogger(HbaseUtils.class);

//    public static final String HbaseZookeeper="192.168.187.21,192.168.187.22,192.168.187.23";
    public static final String HbaseZookeeper=ConfigProperties.HBASE_ZOOKEEPER_QUORUM;
    public static final String HbaseZookeeperPoit=ConfigProperties.HBASE_ZOOKEEPERP_ROPERTY_CLIENTPORT;
    public static final String HbaseZnode=ConfigProperties.ZOOKEEPR_ZNODE_PARENT;

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;



    //初始化链接
    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", HbaseZookeeper);
        configuration.set("hbase.zookeeper.property.clientPort",HbaseZookeeperPoit);
        configuration.set("zookeeper.znode.parent",HbaseZnode);

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void creatRWSTable(String tableName) throws IOException {
        createTable(tableName,new String[]{ ConfigProperties.RWS_COLFAMILY1,ConfigProperties.RWS_COLFAMILY2});
    }

    public static void createIndexTable(String tableName) throws IOException {
        createTable(tableName,new String[]{ConfigProperties.INDEX_COLFAMILY1,ConfigProperties.INDEX_COLFAMILY2});
    }


    //建表
    public static void createTable(String tableNmae,String[] cols) throws IOException {

        init();
        TableName tableName = TableName.valueOf(tableNmae);

        if(admin.tableExists(tableName)){
            LOG.info("数据库已经存在");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col:cols){
                if(StringUtils.isEmpty(col)){
                    LOG.info(col+"列族不存在");
                    continue;
                }
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            LOG.info("数据库构建成功");
        }
        close();
    }

    //删表
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    //查看已有表
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    //插入数据
    public static void insterRow(String tableName,String rowkey,String colFamily,String col,String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
        close();
    }

    //删除数据
    public static void deleRow(String tableName,String rowkey,String colFamily,String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        //批量删除
       /* List<Delete> deleteList = new ArrayList<Delete>();
        deleteList.add(delete);
        table.delete(deleteList);*/
        table.close();
        close();
    }

    //根据rowkey查找数据
    public static void getData(String tableName,String rowkey,String colFamily,String col)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        close();
    }

    //格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }

    //批量查找数据
    public static void scanData(String tableName,String startRow,String stopRow)throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //scan.setStartRow(Bytes.toBytes(startRow));
        //scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            showCell(result);
        }
        table.close();
        close();
    }

}
