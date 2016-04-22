*usage:* 
1) mvn clean package
2) yarn jar scalding-ali-1.0-SNAPSHOT.jar -Dmapreduce.output.fileoutputformat.compress=true -Davro.output.codec=snappy com.placeiq.scalding.ObservationConverterJob --hdfs --input /some/input --output /some/output
