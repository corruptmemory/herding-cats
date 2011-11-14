/**
 * build.scala
 *
 * @author <a href="mailto:jim@corruptmemory.com">Jim Powers</a>
 *
 * Copyright 2011 Jim Powers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._

object BuildSettings {
  val buildOrganization = "com.corruptmemory"
  val buildScalaVersion = "2.9.1"
  val buildVersion      = "0.1.0-SNAPSHOT"

  val buildSettings = Defaults.defaultSettings ++
                      Seq (organization := buildOrganization,
                           scalaVersion := buildScalaVersion,
                           version      := buildVersion,
                           shellPrompt  := ShellPrompt.buildShellPrompt,
                           autoCompilerPlugins := true,
                           scalacOptions += "-P:continuations:enable")
}

object ShellPrompt {

  object devnull extends ProcessLogger {
    def info (s: => String) {}
    def error (s: => String) { }
    def buffer[T] (f: => T): T = f
  }

  val current = """\*\s+(\w+)""".r

  def gitBranches = ("git branch --no-color" lines_! devnull mkString)

  val buildShellPrompt = {
    (state: State) => {
      val currBranch = current findFirstMatchIn gitBranches map (_ group(1)) getOrElse "-"
      val currProject = Project.extract (state).currentProject.id
      "%s:%s:%s> ".format (currProject, currBranch, BuildSettings.buildVersion)
    }
  }
}

object Resolvers {
  val corruptmemoryUnfilteredRepo = "repo.corruptmemory.com" at "http://corruptmemory.github.com/Unfiltered/repository"
  val jbossResolver = "jboss repo" at "http://repository.jboss.org/nexus/content/groups/public-jboss"
  val repo1Resolver = "repo1" at "http://repo1.maven.org/maven2"
  val javaNetResolvers = "Java.net Maven 2 Repo" at "http://download.java.net/maven/2"
  // val thirdParty = "Third Party" at "http://aws-gem-server1:8081/nexus/content/repositories/thirdparty"
}

object Dependencies {
  val scalaCheckVersion = "1.9"
  val scalaZVersion = "6.0.3"
  val zookeeperVersion = "3.3.3"

  val scalaz = "org.scalaz" %% "scalaz-core" % scalaZVersion
  val scalaCheck = "org.scala-tools.testing" %% "scalacheck" % scalaCheckVersion % "test"
  val zookeeper = "org.apache.zookeeper" % "zookeeper" % zookeeperVersion
  val continuationsPlugin = compilerPlugin("org.scala-lang.plugins" % "continuations" % "2.9.1")
}

object ArticleServiceBuild extends Build {
  val buildShellPrompt = ShellPrompt.buildShellPrompt

  import Dependencies._
  import BuildSettings._
  import Resolvers._

  val coreDeps = Seq(scalaz,scalaCheck,zookeeper,continuationsPlugin)

  lazy val herdingCats = Project("herding-cats",file("."),
                                    settings = buildSettings ++ Seq(name := "Herding Cats",
                                                                    scalacOptions += "-deprecation",
                                                                    // scalacOptions += "-Xprint:-4",
                                                                    libraryDependencies := coreDeps,
                                                                    resolvers ++= Seq(jbossResolver,javaNetResolvers,corruptmemoryUnfilteredRepo,/*thirdParty,*/repo1Resolver)))
}
