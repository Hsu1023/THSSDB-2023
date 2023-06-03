# ThssDB-2023
ThssDB 2023

## 《数据库原理》2023春大作业要求
https://apache-iotdb.feishu.cn/docx/EuVyd4o04oSHzZxRtBFcfRa0nab

## 《数据库原理》ThssDB2023 开发指南
https://apache-iotdb.feishu.cn/docx/RHnTd3Y3tocJQSxIIJFcDmlHnDd



## 性能调优

> page setting: page size = 8KB, fanout = 65
>
> compilation setting: java 8 -ea -Xmx32M -Xms16M
>
> code: BPlusTreeTest.java

| test size         | 1k                   | 10k                     | 100k                    |
|-------------------|----------------------|-------------------------|-------------------------|
| 内存 (raw version)  | 11ms/3ms/3ms         | 25ms/13ms/88ms          | 0.251s/0.224s/7.454s    |
| RAF读取+无优化         | 2.392s/4.223s/2.453s | 31.980s/44.954s/23.344s | 10min34s/-/-            |
| buf缓冲一个页面，RAF整页读写 | 434ms/635ms/330ms    | 4.796s/7.556s/3.727s    | 45.927s/76s/45.901s     |
| buffer pool = 4   | 173ms/146ms/86ms     | 1.428s/2.100s/1.164s    | 13.600s/22.685s/33.803s |
| buffer pool = 32  | 128ms/74ms/49ms      | 1.054s/1.533s/0.967s    | 10.415s/15.826s/30.455s |
| buffer pool = 1024| 152ms/73ms/65ms      | 767ms/781ms/658ms       | 9.478s/13.725s/29.912s  |

> 注：X/Y/Z分别代表testGet/testRemove/testIterator的时间
