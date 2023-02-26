# 第六章 期中大作业-task05

> 【学习资料】[第46期组队学习-妙趣横生大数据(Juicy Big Data)](https://datawhalechina.github.io/juicy-bigdata)

## 6. 1 面试题

### 6.1.1 简述Hadoop小文件弊端

**答**：由于HDFS无法高效存储和处理大量的小文件，过多小文件会给系统扩展性和性能带来诸多问题：

1. HDFS采用名称节点（NameNode）来管理文件系统的元数据，这些元数据被保存在内存中，使客户端可以快速获取文件实际存储位置。通常，每个文件、目录和块大约占150字节，一个文件对应一块，这些均为元数据，文件数量越多，元数据信息越多，占据内存空间就越大。这时元数据检索的效率就比较低，需要花费较多的时间找到一个文件的实际存储位置。而且，当继续扩展到数十亿个文件时，名称节点保存元数据所需要的内存空间就会大大增加，以现有的硬件水平，是无法在内存中保存如此大量的元数据；
2. 用MapReduce处理大量小文件时，会产生过多的Map任务，线程管理开销会大大增加，因此处理大量小文件的速度远远低于处理同等大小的大文件的速度；
3. 访问大量小文件的速度远远低于访问大文件的速度，因为访问大量小文件，需要不断从一个数据节点跳到另一个数据节点，严重影响性能。

### 6.1.2 HDFS中DataNode挂掉如何处理？

**答**：每个数据节点会定期向名称节点发送“心跳（heartbeat）”信息，向名称节点报告自己的状态。当数据节点发生故障，或者网络发生断网时，名称节点就无法        收到来自这些节点的“心跳”信息，这时，这些节点就会被标记为“宕机”，节点上面的数据都会被标记为“不可读”，名称节点不会再给它们发送任何I/O请求。当        名称节点检查发现，某个数据的副本数量小于冗余因子，就会启动数据冗余复制，为它生成新的副本。

### 6.1.3 HDFS中NameNode挂掉如何处理？

答：Hadoop一般结合使用下述两种方法来确保名称节点的安全：
        把名称节点上的元数据信息同步存储到其他文件系统中；
        运行一个第二名称节点，当名称节点宕机以后，利用第二名称节点中的元数据信息进行系统恢复。
        当名称节点宕机时，首先到远程挂载的网络文件系统中获取备份的元数据信息，放到第二名称节点上进行恢复，并把第二名称节点作为名称节点来使用。

### 6.1.4 HBase读写流程？

**答**：用户写入数据时，被分配到相应Region服务器去执行；用户数据首先被写入到MemStore和Hlog中，只有当操作写入Hlog之后，调用commit方法才会将其返        回给客户端。
        用户读取数据时， Region服务器会首先访问MemStore缓存，如果找不到，再到磁盘的StoreFile中寻找。

### 6.1.5 MapReduce为什么一定要有Shuffle过程

**答**：为了让Reduce可以并行处理Map的结果，所以要有Shuffle过程，即对`Map`的输出进行一定的分区、排序（Sort）、合并（Combine）和归并（Merge）等操        作，得到`<key,value-list>`形式的中间结果。

### 6.1.6 MapReduce中的三次排序

**答**：Map端的Shuffle过程的溢写操作：对于每个分区内的所有键值对，后台线程会根据key对它们进行内存排序（Sort）；
        Map端的Shuffle过程的文件归并操作：在Map任务全部结束之前，系统会对所有溢写文件中的数据进行归并（Merge），生成一个大的溢写文件，这个大的        溢写文件中的所有键值对，也是经过分区和排序的；
        Reduce端端Shuffle过程的归并数据操作：从Map端领回的数据，多个溢写文件会被归并成一个大文件，归并的时候还会对键值对进行排序，从而使得最终大        文件中的键值对都是有序的。

### 6.1.7 MapReduce为什么不能产生过多小文件

答：MapReduce默认情况下使用TextInputFormat 切片，其机制：
        1 简单地按照文件的内容长度进行切片
        2 切片大小，默认等于Block大小，可单独设置
        3 切片时不考虑数据集整体，而是逐个针对每一个文件单独切片 MapTask
        因此如果有大量小文件，就会产生大量的MapTask，处理效率极其低下。

## 6.2 实战

### 6.2.1 数据集

数据集包含用户的签到历史，其中每条记录的格式为“ userID，locID，check_in_time”，其中 userID (字符串类型)是用户的 ID，locID (字符串类型)是位置的 ID，check _ in _ time (字符串类型)是用户在该位置签到的时间戳。示例文件如下:

```
u1,l1,t1
u1,l1,t2 
u1,l2,t3 
u2,l1,t4 
u2,l3,t5 
u3,l2,t6 
u3,l2,t7 
u3,l3,t8 
```

### 6.2.2 代码

目前代码仅编写出来，未来得及在集群上运行，往后会接着尝试到集群上跑一下

```python
from mrjob.job import MRJob
from mrjob.step import MRStep


class Project1(MRJob):
    def mapper(self, _, line):
        userID, locID, time = line.split(",")
        #填入mapper的具体步骤
        yield userID, 1 
        yield locID, 1 
        yield time, 1


    def combiner(self, key, values):
        #填入combiner的具体步骤
        pass


    def reducer_init(self):
        # 填入reducer_init的具体步骤
        pass


    def reducer(self, key, values):
        userID, locID = key.split(",")
        # 填入reducer的具体步骤
        yield key, sum(values)


    def reducer_sort(self, key, _):
        userID, locID, v = key.split("#")
        yield locID, f'{userID},{v}'

    SORT_VALUES = True

    def steps(self):
        #填入配置参数
        JOBCONF1 = {
            'mapreduce.map.output.key.field.separator':','
            'mapreduce.partition.keypartitioner.options':'-k1,1 -k2,2',
            # Below is not necessary, but you can still do it
            # 'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            # 'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2',
        }
        # 填入配置参数
        JOBCONF2 = {
            'mapreduce.map.output.key.field.separator':'\t',
            'mapreduce.partition.keypartitioner.options':','
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options':'-k1,1'
        }
        return [
            MRStep(jobconf=JOBCONF1, mapper=self.mapper, combiner=self.combiner, reducer_init=self.reducer_init,
                   reducer=self.reducer),
            MRStep(jobconf=JOBCONF2, reducer=self.reducer_sort)
        ]


if __name__ == '__main__':
    Project1.run()
```
