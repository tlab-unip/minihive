
### Docker Instructions

Pull the image
```
docker pull ghcr.io/sdbs-uni-p/minihive-docker@sha256:fbf936d6ea86c7fb53c0833ea2d981f2b066943c8fc625c66a79c138d8262531
docker tag eb2621fb2b4f minihive-docker:latest
```

Run with shared directory
```
docker run -d --name minihive -p 2222:22 -v /home/{$USER}/schoolwork/minihive:/home/minihive/minihive minihive-docker
```

Run test command
```
docker exec minihive python /home/minihive/minihive/milestone-sample/test_example.py
```

### MapReduce Instructions

Run the job
```
mapred streaming -mapper mapper.py -reducer reducer.py -file <relative-path-to-mapper.py> -file <relative-path-to-reducer.py> -input <hdfs-path-to-source> -output <hdfs-path-to-output>
```

Test the script
```
head -50 ../data/purchases.txt > testfile
cat testfile | ./mapper.py | sort | ./reducer.py
```


### Milestones

