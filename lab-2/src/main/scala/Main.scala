import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

object Main {
  def main(args: Array[String]): Unit = {
    val hdfsUri = "hdfs://localhost:9000"

    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    conf.set("dfs.replication", "1")

    val fs = FileSystem.get(new URI(hdfsUri), conf)

    val path = new Path("/")
    val status = fs.listStatus(path)

    println("Содержимое корневого каталога HDFS:")

    status.foreach(x => println(x.getPath))

    fs.close()
  }
}

