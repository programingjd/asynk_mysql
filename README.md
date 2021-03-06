![jcenter](https://img.shields.io/badge/_jcenter_-0.0.0.34-6688ff.png?style=flat) &#x2003; ![jcenter](https://img.shields.io/badge/_Tests_-83/83-green.png?style=flat)
# Asynk MYSQL
A Mysql/Mariadb async client with suspend functions for kotlin coroutines.

## Download ##

The maven artifacts are on [Bintray](https://bintray.com/programingjd/maven/info.jdavid.asynk.mysql/view)
and [jcenter](https://bintray.com/search?query=info.jdavid.asynk.mysql).

[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/asynk/mysql/0.0.0.34/mysql-0.0.0.34.jar) the latest jar.

__Maven__

Include [those settings](https://bintray.com/repo/downloadMavenRepoSettingsFile/downloadSettings?repoPath=%2Fbintray%2Fjcenter)
 to be able to resolve jcenter artifacts.
```
<dependency>
  <groupId>info.jdavid.asynk</groupId>
  <artifactId>mysql</artifactId>
  <version>0.0.0.34</version>
</dependency>
```
__Gradle__

Add jcenter to the list of maven repositories.
```
repositories {
  jcenter()
}
```
```
dependencies {
  compile 'info.jdavid.asynk:mysql:0.0.0.34'
}
```

## Limitations ##

 - Sending or receiving
[packets larger than 16MB](https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html)
is not supported.
 - SSL is not supported.

## Usage ##

__Connecting__

```kotlin
val credentials = 
  MysqlAuthentication.Credentials.PasswordCredentials("user", "password")
val serverAddress = InetAddress.getByName("hostname")
println(
  runBlocking {
    credentials.connectTo("database", InetSocketAddress(serverAddress, db.port)).use { connection ->
      // returns a list of one row as a map of key (column name or alias) to value.
      connection.rows("SELECT VERSION();").toList().first().values.first()
    }
  }
)
```

__Fetching rows__

```kotlin
val total: Int = 
  connection.values("SELECT COUNT(*) as count FROM table;", "count").iterate {
    it.next() // only one row
  }

val max = 123
val count: Int =
  connection.values(
    "SELECT COUNT(*) as count FROM table WHERE id > ?;",
    listOf(max),
    "count"
  ).iterate {
    it.next() // only one value
  }

val ids = ArrayList<Int>(count)
connection.values("SELECT * FROM table WHERE id > ?", listOf(max), "id").toList(ids)

val names = Map<Int,String?>(count)
connection.entries("SELECT id, name FROM table", "id", "name").toMap(map)

val json = com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(
  connection.rows("SELECT * FROM table").toList(ids)
)
```

__Updating rows__

```kotlin
println(
  connection.affectedRows("UPDATE table SET key = ? WHERE id = ?", listOf("a", 1))
)

println(
  connection.prepare("DELETE FROM table WHERE id = ?").use { statement ->
    listOf(1, 3, 4, 6, 9).count { id ->
      statement.affectedRows(listOf(id)) == 1
    }
  }
)
```

## Tests ##

The unit tests assume that docker is running and that the docker daemon is exposed on tcp port 2375.
This option is not enabled by default.
On windows, you will also get prompts to give permission to share your drive that you have to accept.

![Docker settings](./docker_expose_daemon.png)