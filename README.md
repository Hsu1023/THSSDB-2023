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

| test size                    | 1k                   | 10k                     | 100k                 |
| ---------------------------- | -------------------- | ----------------------- | -------------------- |
| 内存 (raw version)           | 11ms/3ms/3ms         | 25ms/13ms/88ms          | 0.251s/0.224s/7.454s |
| RAF读取+无优化               | 2.392s/4.223s/2.453s | 31.980s/44.954s/23.344s | 10min34s/-/-         |
| buf缓冲一个页面，RAF整页读写 | 434ms/635ms/330ms    | 4.796s/7.556s/3.727s    | 45.927s/76s/45.901s  |

> 注：X/Y/Z分别代表testGet/testRemove/testIterator的时间

