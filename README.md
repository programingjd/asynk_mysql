![jcenter](https://img.shields.io/badge/_jcenter_-0.0.0.3-6688ff.png?style=flat) &#x2003; ![jcenter](https://img.shields.io/badge/_Tests_-69/69-green.png?style=flat)
# Asynk MYSQL
A Mysql/Mariadb async client with suspend functions for kotlin coroutines.

## Download ##

The maven artifacts are on [Bintray](https://bintray.com/programingjd/maven/info.jdavid.asynk.mysql/view)
and [jcenter](https://bintray.com/search?query=info.jdavid.asynk.mysql).

[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/asynk/mysql/0.0.0.3/mysql-0.0.0.3.jar) the latest jar.

__Maven__

Include [those settings](https://bintray.com/repo/downloadMavenRepoSettingsFile/downloadSettings?repoPath=%2Fbintray%2Fjcenter)
 to be able to resolve jcenter artifacts.
```
<dependency>
  <groupId>info.jdavid.asynk.mysql</groupId>
  <artifactId>mysql</artifactId>
  <version>0.0.0.3</version>
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
  compile 'info.jdavid.asynk:mysql:0.0.0.3'
}
```