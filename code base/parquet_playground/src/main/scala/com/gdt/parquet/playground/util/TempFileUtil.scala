package com.gdt.parquet.playground.util

import java.nio.file.Files

/**
  * Copyright 2015 Tencent Inc.
  * @author lwlin <lwlin@tencent.com>
  */
trait TempFileUtil {

  def tempDir(prefix: String) = {
    val tempDir = Files.createTempDirectory(prefix)
    tempDir.toFile.deleteOnExit()
    tempDir
  }
}