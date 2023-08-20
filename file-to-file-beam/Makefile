test:
	mvn test

jar:
	mvn clean package

run-local-sideinput:
	mvn compile exec:java -Dexec.mainClass=com.michael.beam.SideinputPipeline \
		-Pdirect-runner \
 		-Dexec.args="--input=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian \
                     --output=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/tmp/output"


run-local-cogroupbykey:
	mvn compile exec:java -Dexec.mainClass=com.michael.beam.CoGroupByKeyPipeline \
		-Pdirect-runner \
 		-Dexec.args="--input=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian \
                     --output=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/tmp/output"

run-flink-sideinput:
	mvn package exec:java -Dexec.mainClass=com.michael.beam.SideinputPipeline \
      -Pflink-runner \
      -Dexec.args="--runner=FlinkRunner \
       --input=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian \
       --output=/tmp/output \
       --flinkMaster=http://127.0.0.1:8081 \
       --filesToStage=target/file-to-file-beam-bundled-0.1.jar"

