1.本项目主要功能是将hive表中已经join好的数据转存到hbase数据库中，分别存成两张表。
表一主要为rws项目支持的hbase数据库，数据形式为 
rowkey：patient_sn 
colFamily1:patient_sn  
col:patient:_info
value1:patient{..}
colFamily2:visit_info
col 以及value 为 原visit 下的所有列
value2: 将相同的列进行合并，最后作为每个列的value

表二为建索引提供
rowkey:patient_sn
colFamily1:patient_sn
col:data
value: 原hive表中data数据集

执行命令：
hadoop jar xxxx.jar db=[dbName] table=[tableName] rwst=[RWS_HBASE_TableName] index[INDEX_HBASE_TableName]
更新时间 2018年3月22日