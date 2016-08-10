## 1. 获取运行的依赖包,并放到hss-lib目录下
新建目录

``mkdir hss-lib``

查看依赖包

``sbt``

``show compile:dependencyClasspath``

把依赖包的字符串复制下来，使用scala进行处理，生成依赖包的复制命令

``
$ scala
val output = """List(Attributed(/Users/cookeem/.ivy2/cache/org.scala-lang/scala-library/jars/scala-library-2.11.8.jar), Attributed(/Users/cookeem/.ivy2/cache/mysql/mysql-connector-java/jars/mysql-connector-java-5.1.38.jar))"""

val arr = output2.replace("List","").replace("Attributed","").replace("(","").replace(")","").replace(" ","").split(",")

val cmd = arr.filter(!_.endsWith("_2.10-1.6.1.jar")).map(s => s"cp $s hss-lib/").mkString("\n")

println(cmd)
``

把依赖包复制到hss-lib目录

## 2. sbt编译程序

``sbt clean package``


## 3. 复制hss-lib目录到所有executor上

## 4. 提交spark-submit

``spark-submit --driver-class-path "hss-lib/*" --class cmgd.zenghj.hss.spark.KafkaSinkEs target/scala-2.10/hss-spark_2.10-1.0.jar``

--driver-class-path: 用于指定driver的classpath, 否则会出现spark内置jar包的版本冲突
在程序中设置spark.executor.extraClassPath以及spark.executor.extraLibraryPath参数,保证executor不会出现jar包版本冲突

`
  val conf = new SparkConf().setMaster(configSparkMaster).setAppName(configSparkAppName)
  
  val hssLibFile = new File("hss-lib")
  
  val hssLibPath = hssLibFile.getAbsolutePath
  
  //通过设置executor的classpath保证executor上也可以执行高版本KryoInjection
  
  conf.set("spark.executor.extraClassPath", s"$hssLibPath/*")
  
  conf.set("spark.executor.extraLibraryPath", s"$hssLibPath/*")
`  
