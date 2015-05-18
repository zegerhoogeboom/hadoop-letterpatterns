@ECHO OFF
mvn install &  rd /s /q "output" & hadoop jar .\target\letterpatterns-1.0-SNAPSHOT.jar LetterPatternJob input output
