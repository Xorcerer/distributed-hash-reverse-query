分布式md5生成，查询框架
============

运行
----

启动分发队列：
	python message_queue/server.py

启动工作进程：
    python worker.py <处理md5的前缀，如ea> <leveldb数据库的名称，如 ea.leveldb> <分发队列的端口的host> <分发队列的端口>


测试写入
-------
启动上述 server.py 和 worker.py 后，
执行
    python fill_db.py
