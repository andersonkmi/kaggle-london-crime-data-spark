package org.sharpsw.spark.utils

import java.io.File.separator
import java.io.{File, FileInputStream, FileOutputStream}

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import org.apache.log4j.Logger

object S3Util {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)
  private val s3Service = AmazonS3ClientBuilder.standard.build

  def downloadObject(bucket: String, key: String, local: String = "."): Unit = {
    val o = s3Service.getObject(bucket, key)
    val s3is = o.getObjectContent
    val tokens = key.split("/")

    val path = new File(local)
    val fos = new FileOutputStream(new File(path.getAbsolutePath + separator + tokens.last))
    val read_buf = new Array[Byte](1024)
    var len = s3is.read(read_buf)
    while (len > 0) {
      fos.write(read_buf, 0, len)
      len = s3is.read(read_buf)
    }
    s3is.close()
    fos.close()
  }

  def uploadFiles(bucket: String, prefix: String, localBasePath: String, files: List[String]): Unit = {
    files.foreach(item => uploadSingleFile(bucket, prefix + item.substring(localBasePath.length).replaceAll("\\\\", "/"), item))
  }

  private def uploadSingleFile(bucket: String, key: String, uploadFileName: String): Unit = {
    try {
      logger.info(s"Uploading file $uploadFileName to S3")
      val file = new File(uploadFileName)
      val is = new FileInputStream(file)
      val metadata = new ObjectMetadata()
      metadata.setContentLength(file.length())
      s3Service.putObject(new PutObjectRequest(bucket, key, is, metadata))
      is.close()
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }
}
