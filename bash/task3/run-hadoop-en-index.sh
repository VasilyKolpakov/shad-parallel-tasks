#!/bin/bash
hadoop jar parallel-task3-1.0-jar-with-dependencies.jar ru.vasily.shad.parallel.task3.Index \
-D lang=en -D reducers=70 -D topPath=/user/kolpakov/top20/en/output/2_top_getter/part-r-00000 \
-D mapred.job.queue.name=single /data/wiki/en/articles /user/kolpakov/index/en/output
