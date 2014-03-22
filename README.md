分布式md5生成，查询框架
============

依赖
----

* pyzmq
* leveldb-py

运行
----

启动消息队列 （默认任务接收端口 7700，分发端口 7701）：

	python message_queue/server.py


启动工作进程：

    python worker.py <处理md5的前缀，如ea> <leveldb数据库的名称，如ea.leveldb> <消息队列的分发端口的host> <消息队列的分发端口>

如：

    python worker.py ea ea.leveldb localhost 7701

    表示只处理（包括保存和查询）ea开头的md5请求，对应数据库ea.leveldb，任务来源localhost:7701

测试
-------
启动上述 server.py 和 worker.py 后，
测试写入：执行以下脚本写入 A-Za-z0-9单字符的md5

    python test_fill_db.py

测试读取：执行以下脚本查询字符 9 对应md5的原文（也就是‘9’）

    python test_lookup_db.py
