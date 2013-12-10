libraryDependencies ++= Seq(
  "com.twitter"             %% "scalding-core"      % "0.9.0rc1",
  "com.twitter"             %% "algebird-core"      % "0.3.0",
  "org.scalaz"              %% "scalaz-core"        % "7.0.4",
  "org.specs2"              %% "specs2"             % "2.3.3"        % "test",
  "org.scalacheck"          %% "scalacheck"         % "1.10.0"       % "test",
  "org.pegdown"             %  "pegdown"            % "1.2.1"        % "test",
  "junit"                   %  "junit"              % "4.11"         % "test"
)

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Clojars Repository" at "http://clojars.org/repo",
  "Twitter Maven" at "http://maven.twttr.com"
)
