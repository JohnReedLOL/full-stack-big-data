import AssemblyKeys._

assemblySettings

// Any customized settings must be written here, i.e. after 'assemblySettings' above.
// See https://github.com/sbt/sbt-assembly for available parameters.

// Include "provided" dependencies back to run/test tasks' classpath.
// See:
// https://github.com/sbt/sbt-assembly#-provided-configuration
// http://stackoverflow.com/a/21803413/3827
//
// In our case, the Storm and Spark dependencies must be set to "provided (cf. `build.sbt`) because, when deploying and
// launching our Storm/Spark jobs "for real" to distributed clusters, Storm/Spark want us to exclude the Storm/Spark
// dependencies (jars) as they are provided [no pun intended] by the respective clusters.
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith(".default") => MergeStrategy.last
    case s if s.endsWith(".map") => MergeStrategy.last
    case s if s.endsWith(".providers") => MergeStrategy.last
    case s if s.endsWith(".properties") => MergeStrategy.last
    case s if s.endsWith(".RSA") => MergeStrategy.last
    case s if s.endsWith("mailcap") => MergeStrategy.last
    case x => old(x)
  }
}

// We do not want to run the test when assembling because we prefer to chain the various build steps manually, e.g.
// via `./sbt clean test scoverage:test package packageDoc packageSrc doc assembly`.  Because, in this scenario, we
// have already run the tests before reaching the assembly step, we do not re-run the tests again.
//
// Comment the following line if you do want to (re-)run all the tests before building assembly.
test in assembly := {}

logLevel in assembly := Level.Warn
