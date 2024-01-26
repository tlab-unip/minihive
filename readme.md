### Docker Instructions

Pull the image

```
docker pull ghcr.io/sdbs-uni-p/minihive-docker@sha256:fbf936d6ea86c7fb53c0833ea2d981f2b066943c8fc625c66a79c138d8262531
docker tag eb2621fb2b4f minihive-docker:latest
```

Run with shared local directory

```
docker run -d --name minihive \
    -v /home/{$USER}/schoolwork/minihive:/home/minihive/minihive \
    minihive-docker
```

Enter the container interactively

```
docker exec -it minihive bash
```

### MapReduce Instructions

Run MapReduce job on Hadoop

```
mapred streaming \
    -mapper mapper.py \
    -reducer reducer.py \
    -file <relative-path-to-mapper.py> \
    -file <relative-path-to-reducer.py> \
    -input <hdfs-path-to-source> \
    -output <hdfs-path-to-output>
```

Simulate MapReduce job locally

```
head -50 path/to/purchases.txt > testfile
cat testfile | ./mapper.py | sort | ./reducer.py
```

### Running Luigi Tasks

Run test cases

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

Run miniHive locally

```
python3 src/miniHive.py \
    --O --SF 0.01 --env LOCAL \
    "select distinct N_NAME from NATION"
```
