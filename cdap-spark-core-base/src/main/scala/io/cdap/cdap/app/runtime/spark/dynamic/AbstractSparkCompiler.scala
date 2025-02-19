/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.app.runtime.spark.dynamic

import io.cdap.cdap.api.spark.dynamic.CompilationFailureException
import io.cdap.cdap.api.spark.dynamic.SparkCompiler
import io.cdap.cdap.common.lang.ClassLoaders
import io.cdap.cdap.common.lang.CombineClassLoader
import io.cdap.cdap.common.lang.jar.BundleJarUtil
import io.cdap.cdap.internal.app.runtime.plugin.PluginClassLoader
import io.cdap.cdap.internal.lang.CallerClassSecurityManager
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.URL
import java.util.function.Predicate
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.io.PlainFile

/**
  * Abstract common base for implementation [[io.cdap.cdap.api.spark.dynamic.SparkCompiler]] for different
  * Spark version.
  */
abstract class AbstractSparkCompiler(settings: Settings, onClose: () => Unit) extends SparkCompiler {

  import AbstractSparkCompiler._

  // Filter out $repl_$init, as newer versions of scala will throw errors for files that already exist on claspath.
  private val REPL_INIT_FILTER = new Predicate[String] {
    def test(name: String): Boolean = {
      return name.equals("$repl_$init.class")
    }
  }

  private val errorReporter = new ErrorReporter()
  private lazy val iMain = createIMain(settings, errorReporter)
  private var compileCount = 0

  /**
    * Creates a new instance of [[scala.tools.nsc.interpreter.IMain]].
    *
    * @param settings the settings for the IMain
    * @param errorReporter a [[io.cdap.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler.ErrorReporter]] for
    *                      error reporting from the IMain
    * @return a new instance of IMain
    */
  protected def createIMain(settings: Settings, errorReporter: ErrorReporter): IMain with URLAdder

  /**
    * Returns the directory where the compiler writes out compiled class files.
    *
    * @return a [[scala.tools.nsc.io.AbstractFile]] represeting output directory
    */
  protected def getOutputDir(): AbstractFile

  override def compile(source: String): Unit = {
    compileCount += 1
    compile(new BatchSourceFile(s"<source-string-$compileCount>", source));
  }

  override def compile(file: File): Unit = {
    compile(new BatchSourceFile(new PlainFile(file)))
  }

  override def saveAsJar(file: File): Unit = {
    val output = new BufferedOutputStream(new FileOutputStream(file))
    try {
      saveAsJar(output)
    } finally {
      output.close()
    }
  }

  override def saveAsJar(outputStream: OutputStream): Unit = {
    val outputDir = getOutputDir()
    val jarOutput = new JarOutputStream(outputStream)
    try {
      // If it is file, use the BundleJarUtil to handle it
      if (outputDir.isInstanceOf[PlainFile]) {
        BundleJarUtil.addToArchive(outputDir.file, false, jarOutput, REPL_INIT_FILTER)
        return
      }

      // Otherwise, iterate the AbstractFile recursively and create the jar
      val queue = new mutable.Queue[AbstractFile]()
      outputDir.foreach(f => queue.enqueue(f))
      while (queue.nonEmpty) {
        val entry = queue.dequeue()
        val name = entry.path.substring(outputDir.path.length + 1)
        if (entry.isDirectory) {
          // If it is directory, add the entry with name ended with "/". Also add all children entries to the queue.
          jarOutput.putNextEntry(new JarEntry(name + "/"))
          jarOutput.closeEntry()
          entry.foreach(f => queue.enqueue(f))
        } else if (!REPL_INIT_FILTER.test(name)) {
          jarOutput.putNextEntry(new JarEntry(name))
          copyStreams(entry.input, jarOutput)
          jarOutput.closeEntry()
        }
      }
    } finally {
      jarOutput.close()
    }
  }

  override def addDependencies(file: File*): Unit = {
    iMain.addURLs(file.map(_.toURI.toURL): _*)
  }

  override def getIMain(): IMain = {
    return iMain
  }

  override def close(): Unit = {
    iMain.reset()
    iMain.close()
    onClose()
  }

  /**
    * Compiles using content defined by the given [[scala.reflect.internal.util.SourceFile]].
    */
  private def compile(sourceFile: SourceFile): Unit = {
    if (!iMain.compileSources(sourceFile)) {
      throw new CompilationFailureException(errorReporter.toString)
    }
  }

  /**
    * Class for reporting errors generated from [[scala.tools.nsc.interpreter.IMain]].
    */
  protected final class ErrorReporter {

    private val errors = new mutable.ListBuffer[String]

    def report(msg: String): Unit = {
      errors += msg
    }

    def clear(): Unit = {
      errors.clear()
    }

    override def toString: String = {
      errors.mkString(System.getProperty("line.separator"))
    }
  }
}

/**
  * Companion object to provide helper methods.
  */
object AbstractSparkCompiler {

  /**
    * Setup the [[scala.tools.nsc.settings]] user classpath based on the context classloader
    * as well as the caller class.
    *
    * @param settings the settings to modify
    * @return the same settings instance from the argument
    */
  def setClassPath(settings: Settings): Settings = {
    // Find the plugin classloader if it is called from plugin
    val classLoaderOption = Option.apply(CallerClassSecurityManager.findCallerClassLoader(classOf[PluginClassLoader]))
    val contextClassLoader = Thread.currentThread().getContextClassLoader

    // Use a combine classloder of plugin + context if the call is from plugin,
    // otherwise just use the context classloader
    val classLoader = classLoaderOption
      .map(cl => new CombineClassLoader(null, List(cl, contextClassLoader)))
      .getOrElse(contextClassLoader)
    settings.embeddedDefaults(classLoader)

    // Adding all classpaths to the compiler settings. We have to exclude directories that ended with .zip or .jar to
    // avoid Scala trying to expand them and fail (due to they are not file).
    // When a directory is ended with .zip or .jar, it is a result of archive expansion on Hadoop, in which we only
    // need the directory content in the classpath, but not the directory itself.
    val classpath = ClassLoaders.getClassLoaderURLs(classLoader, true, new java.util.LinkedHashSet[URL]).toSet[URL]
    settings.classpath.value = classpath
      .map(url => new File(url.getPath()))
      .filter(f => f.isFile || !(f.getName.endsWith(".zip") || f.getName.endsWith(".jar")))
      .map(_.getAbsolutePath)
      .mkString(File.pathSeparator)
    settings
  }

  /**
    * Copying data from one stream to another.
    */
  private def copyStreams(from: InputStream, to: OutputStream): Unit = {
    val buf = new Array[Byte](8192)
    var len = from.read(buf)
    while (len >= 0) {
      to.write(buf, 0, len)
      len = from.read(buf)
    }
  }
}
