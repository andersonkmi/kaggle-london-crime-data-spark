package org.sharpsw.spark.utils

import java.io.File.separator
import java.io.{File, FileOutputStream}

import com.amazonaws.services.s3.AmazonS3ClientBuilder
//import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectMetadata, PutObjectRequest, S3ObjectSummary}

object S3Util {
  private val s3Service = AmazonS3ClientBuilder.standard.build

  def downloadObject(bucket: String, key: String, local: String = "."): Unit = {
    val o = s3Service.getObject(bucket, key)
    val s3is = o.getObjectContent
    val tokens = key.split("/")

    val path = new File(local)
    path.mkdirs()
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
}
