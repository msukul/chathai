@REM start cmd.exe /k "c:/chathai/env/Scripts/python.exe c:/chathai/quiz1/wordcount/producer-harry.py"
START /b /wait cmd /C "C:\kafka_2.13-2.7.0\bin\windows\kafka-console-producer.bat --bootstrap-server "localhost:9092,localhost:9192,localhost:9292" --topic streams-harrycount-input"
