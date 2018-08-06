package org.sharpsw.spark.utils

import java.io.File
import java.nio.file.FileSystems.getDefault

object FileUtil {
  def getListOfFiles(dir: String): List[String] = {
    val root = new File(dir)

    if (root.exists && root.isDirectory) {
      getRecursiveListOfFiles(root).map(item => item.getAbsolutePath).filter(item => item.endsWith(".csv") || item.endsWith(".parquet")).toList
    } else {
      List[String]()
    }
  }

  def getRecursiveListOfFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }


  def buildFilePath(folder: String, fileName: String): String = {
    s"$folder" + getDefault.getSeparator + fileName
  }
}
