# 运行步骤

1、首先执行以下命令编译源码：

```shell
mvn compile
```

2、接着执行以下命令以 G:/Javaweb/workspace4idea/NovaDB/tmp/echodb 作为路径创建数据库（tmp/echodb 文件夹要预先创建好）：

```shell
mvn exec:java@run-server "-Dexec.args=-create G:/Javaweb/workspace4idea/EchoDB/tmp/echodb"
```

3、随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java@run-server "-Dexec.args=-open G:/Javaweb/workspace4idea/EchoDB/tmp/echodb"
```

4、这时数据库服务就已经启动在本机的 9999 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java@run-client
```

会启动一个交互式命令行，就可以在这里输入类 SQL 语法，回车会发送语句到服务，并输出执行的结果。



# 测试结果

```
:> create table student id int 32, name string, age int32 (index id)
Invalid statement: create table student id int << 32, name string, age int32 (index id)
执行失败，原因：int 32不符合类型规范
--------------------------------------------------------------------------------------
:> create table student id int32, name string, age int32 (index id)
create student
执行成功，新建student表，字段如下：id(int32)、name(string)、age(int32)，以id作为索引
--------------------------------------------------------------------------------------
:> insert into student values 10 xiaoming 22
insert
执行成功，插入一名同学
--------------------------------------------------------------------------------------
:> select * from student where id=1
执行成功，不存在id=1的记录
--------------------------------------------------------------------------------------
:> select * from student where id=10
[10, xiaoming, 22]
执行成功，查找出一条记录
--------------------------------------------------------------------------------------
:> begin
begin
:> insert into student values 20 xiaohong 30
insert
:> commit
commit
:> select * from student where id>0
[10, xiaoming, 22]
[20, xiaohong, 30]
执行成功，事务提交
--------------------------------------------------------------------------------------
:> begin
begin
:> delete from student where id=10
delete 1
:> abort
abort
:> select * from student where id>0
[10, xiaoming, 22]
[20, xiaohong, 30]
执行成功，事务回滚
--------------------------------------------------------------------------------------
:> delete from student where id=10
delete 1
:> select * from student where id>0
[20, xiaohong, 30]
执行成功，删除一条记录
--------------------------------------------------------------------------------------
:> select * from student where id>0
[10, xiaoming, 50]
[20, xiaohong, 30]

:> update student set name=xiaogang where id=10
update 1
:> select * from student where id>0
[10, xiaogang, 50]
[20, xiaohong, 30]
执行成功，更新一条记录
--------------------------------------------------------------------------------------
:> quit
```

