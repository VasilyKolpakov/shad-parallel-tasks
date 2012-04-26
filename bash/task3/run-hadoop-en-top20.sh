#!/bin/bash
hadoop jar parallel-task3-1.0-jar-with-dependencies.jar ru.vasily.shad.parallel.task3.Top20 \
-D lang=en -D reducers=70 -D words=20 -D mapred.job.queue.name=single \
/data/wiki/en/articles /user/kolpakov/top20/en/output
