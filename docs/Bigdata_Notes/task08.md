# task08-第十章 期中大作业

> 【学习资料】[第46期组队学习-妙趣横生大数据(Juicy Big Data)](https://datawhalechina.github.io/juicy-bigdata)

### 10.1.1 hive外部表和内部表的区别

Hive中的内部表和外部表是不同的类型的表，主要区别在于它们管理数据的方式以及数据存储的位置。

内部表（Internal Table）是在Hive的数据仓库中具有完全控制权的表，数据文件存储在Hive自己的文件系统中。当您创建内部表时，Hive将数据自动存储在其自己的目录结构中，并在表的定义中指定路径。当您删除一个内部表时，Hive会自动删除表的所有数据。

外部表（External Table）则是在Hive之外管理的表。这意味着，外部表中的数据可以实际上存储在您的本地文件系统或Hadoop集群之外的其他文件系统中。当您创建一个外部表时，您需要指定数据文件的位置，如果您删除外部表，数据文件将不会被自动删除，这是外部表和内部表的主要区别。

总的来说，如果您使用Hive来管理数据，并希望控制数据的完整性和管理，那么您应该使用内部表。如果您希望在Hive中访问未受Hive控制的数据，或者有其他程序需要访问数据文件，那么您应该使用外部表。

### 10.1.2 简述对Hive桶的理解？

分桶是针对数据文件本身进行拆分，根据表中字段（例如，编号ID）的值，经过hash计算规则，将数据文件划分成指定的若干个小文件。分桶后，HDFS中的数据文件会变为多个小文件。分桶的优点是优化join查询和方便抽样查询。

### 10.1.3 HBase和Hive的区别？

HBase是一个面向列式存储、分布式、可伸缩的数据库，它可以提供数据的实时访问功能，而Hive只能处理静态数据，主要是BI报表数据。就设计初衷而言，在Hadoop上设计Hive，是为了减少复杂MapReduce应用程序的编写工作，在Hadoop上设计HBase是为了实现对数据的实时访问。所以，HBase与Hive的功能是互补的，它实现了Hive不能提供的功能。

### 10.1.4 简述Spark宽窄依赖

Spark把计算阶段不需要Shuffle的依赖，称为窄依赖。需要Shuffle的依赖，称为宽依赖。

### 10.1.5 Hadoop和Spark的相同点和不同点

Hadoop和Spark两者都是大数据框架，但是各自存在的目的不同。Hadoop实质上是一个分布式数据基础设施，它将巨大的数据集分派到一个集群中的多个节点进行存储，并具有计算处理的功能。Spark则不会进行分布式数据的存储，是计算分布式数据的工具。

### 10.1.6 Spark为什么比MapReduce块？

从本质上，Spark可以算是一种MapReduce计算模型的不同实现，Hadoop MapReduce根据Shuffle将大数据计算分为Map和Reduce两个阶段。而Spark更流畅，将前一个的Reduce和后一个的Map进行连接，当作一个阶段进行计算，从而形成了一个更高效流畅的计算模型。其本质仍然是Map和Reduce。但是这种多个计算阶段依赖执行的方案可以有效减少对HDFS的访问（落盘），减少作业的调度执行次数，因此执行速度也更快。
从存储方式上：MapReduce主要使用磁盘存储Shuffle过程的数据，而Spark优先使用内存进行数据存储（RDD也优先存于内存）。这也是Spark比Hadoop性能高的另一个原因。

### 10.1.7 说说你对Hadoop生态的认识

Hadoop生态是一个开源的大数据处理框架，包括了Hadoop核心组件（HDFS、YARN和MapReduce），以及各种相关工具和库，如HBase、Hive、Pig、Spark、ZooKeeper等。Hadoop生态凭借着其高可靠性、高可扩展性、高吞吐量等优势，在大数据处理领域得到了广泛应用。

Hadoop生态的核心组件HDFS是一个分布式文件系统，它可以将大量数据分布存储在集群中的多个节点上，提高了存储系统的可靠性和容错能力。YARN是资源调度平台，它可以将集群中的计算资源合理地分配给不同的应用程序，确保集群的高效利用。MapReduce是一种分布式计算框架，可以将大规模的数据集分解成多个小块进行并行处理，从而加快计算速度。

## 10.2 实战

从新闻文章中发现热门话题和趋势话题是舆论监督的一项重要任务。在这个项目中，你的任务是使用新闻数据集进行文本数据分析，使用 Python 中 pySpark 的 RDD 和 DataFrame 的 API。问题是计算新闻文章数据集中每年各词的权重，然后选择每年 top-**k**个最重要的词。

**PS：解决方案源码填空示例与结果验证数据在`\juicy-bigdata\experiments\10 期末大作业`目录下**

**同时提前安装`pyspark`包**

### 10.2.1 数据集

您将使用的数据集包含多年来发布的新闻标题数据。在这个文本文件中，每一行都是一篇新闻文章的标题，格式为“ date,term1 term2... ...”。日期和文本用逗号分隔，文本用空格字符分隔。示例文件如下:

```
20030219,council chief executive fails to secure position
20030219,council welcomes ambulance levy decision
20030219,council welcomes insurance breakthrough
20030219,fed opp to re introduce national insurance
20040501,cowboys survive eels comeback
20040501,cowboys withstand eels fightback
20040502,castro vows cuban socialism to survive bush
20200401,coronanomics things learnt about how coronavirus economy
20200401,coronavirus at home test kits selling in the chinese community
20200401,coronavirus campbell remess streams bear making classes
20201015,coronavirus pacific economy foriegn aid china
20201016,china builds pig apartment blocks to guard against swine flu
```

### 10.2.2 文本权重计算

您需要忽略诸如“ to”、“ the”和“ in”之类的停用词。该文件存储了停用词。

为了计算一个文本的为期一年的权重，请使用 TF/IDF 模型。具体而言，TF 和 IDF 可计算为:

$TF(文本 t, 年份 y) = 在 y 年份中，包含文本 t 的新闻标题数量$

$IDF(文本 t,数据集 D) = log10 (在数据集D中年份的总数/含有文本t的年份总数) $

最后，文本 t 对于年份 y 的权重计算如下:

$Weight(文本 t, 年份 y, 数据集 D) = TF(文本 t, 年份 y)* IDF(文本 t, 数据集 D) $

请使用 `import math` 并使用 `math.log10()`计算文本权重，并将结果四舍五入到小数点后6位。

### 10.2.3 输出格式

如果数据集中有 N 年，那么您应该在最终输出文件中输出正好 N 行，并且这些行按年份升序排序。在每一行中，您需要以`<term, weight> `的格式输出 k 对list，并且这些对按照文本权重降序排序。如果两个文本具有相同的权重，则按字母顺序对它们进行排序。具体来说，每行的格式类似于: 
“year**\t** Term1,Weight1;Term 2,Weight2;… …;Termk,Weightk” 。例如，给定上述数据集和 **k** = 3，输出应该是:

```
2003 council,1.431364;insurance,0.954243;welcomes,0.954243
2004 cowboys,0.954243;eels,0.954243;survive,0.954243
2020 coronavirus,1.908485;china,0.954243;economy,0.954243
```

### 10.2.4 运行指令

**Dataframe:**

```
spark-submit project_df.py file:///testcase_top_20/sample.txt file:///res_df file:///stopwords.txt k
```

**RDD:**

```
spark-submit project_rdd.py file:///testcase_top_20/sample.txt file:///res_rdd file:///stopwords.txt k
```

其中 `file://`后跟随本地文件目录地址。
