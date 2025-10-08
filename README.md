# 描述
> 该工具是一个通用的 Java 程序，能够执行 MySQL 和 PostgreSQL 数据库的 SQL 脚本文件，通过灵活的参数配置实现跨数据库类型的脚本执行功能。

```shell
java MultiDbSqlExecutor \
  <数据库类型(mysql/pgsql)> \
  <JDBC连接串> \
  <用户名> \
  <密码> \
  <驱动类名> \
  <SQL脚本路径>
```

```shell
java com.huadis.SQLScriptExecutor mysql "jdbc:mysql://localhost:3306/mydb?useSSL=false&serverTimezone=UTC" root password com.mysql.cj.jdbc.Driver ./mysql_script.sql
```

```shell
java com.huadis.SQLScriptExecutor pgsql "jdbc:postgresql://localhost:5432/mydb" postgres password org.postgresql.Driver ./pgsql_script.sql
```
