
### Docker Instructions

Pull the image
```
docker pull ghcr.io/sdbs-uni-p/minihive-docker@sha256:fbf936d6ea86c7fb53c0833ea2d981f2b066943c8fc625c66a79c138d8262531
docker tag eb2621fb2b4f minihive-docker:latest
```

Run with shared directory
```
docker run -d --name minihive -v /home/{$USER}/schoolwork/minihive:/home/minihive/minihive minihive-docker
```

Enter container interactively
```
docker exec -it minihive bash
```

Run test command
```
docker exec minihive python /home/minihive/minihive/milestone-sample/test_example.py
```

### MapReduce Instructions

Run the job
```
mapred streaming -mapper mapper.py -reducer reducer.py -file <relative-path-to-mapper.py> \
    -file <relative-path-to-reducer.py> -input <hdfs-path-to-source> -output <hdfs-path-to-output>
```

Test the script
```
head -50 ../data/purchases.txt > testfile
cat testfile | ./mapper.py | sort | ./reducer.py
```


### Running Luigi Tasks

```
python3 -m luigi \
    --module src.ra2mr SelectTask \
    --querystring "\select_{gender='female'} Person;" \
    --exec-environment LOCAL --local-scheduler
```

```
python3 -m pytest tests/test_ra2mr.py \
    -p no:warnings --show-capture=no
```

Run optimizing task

```
python3 src/miniHive.py \
    --O --SF 1 --env LOCAL \
    "select distinct N_NAME from NATION"
```
