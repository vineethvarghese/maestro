resolvers ++= Seq(
  Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]")),
  "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"
)

val uniformVersion = "0.0.1-20140319230817-9bf2ec4"

addSbtPlugin("au.com.cba.omnia" % "uniform-core"       % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency" % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-thrift"     % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-assembly"   % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "humbug-plugin"      % "0.0.1-20140513231339-50ed724")
