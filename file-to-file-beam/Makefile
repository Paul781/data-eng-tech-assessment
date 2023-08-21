DOCKER_ID = 4aba8fce0184

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
       --input=/tmp \
       --output=/tmp/output \
       --flinkMaster=http://127.0.0.1:8083 \
       --filesToStage=target/file-to-file-beam-bundled-0.1.jar"

run-jar-sideinput:
	java -cp /Users/michael_sun/IdeaProjects/data-eng-tech-assessment/file-to-file-beam/target/file-to-file-beam-bundled-0.1.jar \
			com.michael.beam.SideinputPipeline \
			--runner=DirectRunner \
			--input=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian \
            --output=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/tmp/output


run-jar-cogroup:
	java -jar /Users/michael_sun/IdeaProjects/data-eng-tech-assessment/file-to-file-beam/target/file-to-file-beam-bundled-0.1.jar \
			--runner=DirectRunner \
			--input=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian \
            --output=/Users/michael_sun/IdeaProjects/data-eng-tech-assessment/tmp/output

cp-to-docker:
	docker cp /Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian/pedestrian-counting-system-monthly-counts-per-hour.json $(DOCKER_ID):/tmp/
	docker cp /Users/michael_sun/IdeaProjects/data-eng-tech-assessment/source-data/pedestrian/pedestrian-counting-system-sensor-locations.json $(DOCKER_ID):/tmp/
