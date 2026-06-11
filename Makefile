run:
	JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" sbt run
