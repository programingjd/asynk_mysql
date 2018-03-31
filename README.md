![jcenter](https://img.shields.io/badge/_jcenter_-1.0.0.4-6688ff.png?style=flat) &#x2003; ![jcenter](https://img.shields.io/badge/_Tests_-1/1-green.png?style=flat)
# mysql
A Mysql/Mariadb async client with suspend functions for kotlin coroutines.

## Download ##

The maven artifacts are on [Bintray](https://bintray.com/programingjd/maven/info.jdavid.mysql/view)
and [jcenter](https://bintray.com/search?query=info.jdavid.mysql).

[Download](https://bintray.com/artifact/download/programingjd/maven/info/jdavid/mysql/mysql/1.0.0.4/mysql-1.0.0.4.jar) the latest jar.

__Maven__

Include [those settings](https://bintray.com/repo/downloadMavenRepoSettingsFile/downloadSettings?repoPath=%2Fbintray%2Fjcenter)
 to be able to resolve jcenter artifacts.
```
<dependency>
  <groupId>info.jdavid.mysql</groupId>
  <artifactId>mysql</artifactId>
  <version>1.0.0.4</version>
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
  compile 'info.jdavid.mysql:mysql:1.0.0.4'
}
```