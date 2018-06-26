package org.sharpsw.spark.utils

import java.io.File

object FileUtil {
  def getListOfFiles(dir: String): List[String] = {
    val root = new File(dir)

    if (root.exists && root.isDirectory) {
      getRecursiveListOfFiles(root).map(item => item.getAbsolutePath).toList
    } else {
      List[String]()
    }
  }

  def getRecursiveListOfFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

}
