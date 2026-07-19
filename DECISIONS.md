## Virtual Nodes

**Goal:** Implement consistent hashing for key partitioning

**Current implementation:**
1. Hash each server name once and place it on a uint64 ring
2. Route each key to the nearest server clockwise using binary search

**Experiment:** Across 8 servers, hash 10,000 keys (FNV-1a 64-bit) and route each to the nearest server on the ring. Below are the results across 3 trials.
Total keys: 10,000 | Ideal distribution: 1,250 keys per server (12.5%)

### Results (sorted)

**Run #1** (8 servers listed)
```
b122197d: 2100 keys (21.0%)  
9928e360: 2000 keys (20.0%)  
0d5da2b8: 1600 keys (16.0%)
588d1c44: 1510 keys (15.1%)
07b2d9d6:  980 keys ( 9.8%)
50287b16:  810 keys ( 8.1%)
7f3d6892:  600 keys ( 6.0%)
e638d3a9:  400 keys ( 4.0%)  
```

**Run #2** (7 servers listed, one server did not recieve any keys)
```
e02160d0: 4800 keys (48.0%)  
a7ce08a0: 1410 keys (14.1%)
e72a1bd2: 1190 keys (11.9%)
5e702e5d:  900 keys ( 9.0%)
f84a2403:  800 keys ( 8.0%)
2a4894d1:  500 keys ( 5.0%)
2160c86d:  400 keys ( 4.0%)  
[missing]: 0 keys ( 0.0%)  
```

**Run #3** (8 servers listed)
```
564b4cd8: 2210 keys (22.1%)  
76b7cb30: 2200 keys (22.0%)
1fb8a90a: 1590 keys (15.9%)
b906b892: 1200 keys (12.0%)
6cd17968:  800 keys ( 8.0%)
6363d4f5:  700 keys ( 7.0%)
e61cb7af:  700 keys ( 7.0%)
75072408:  600 keys ( 6.0%)  
```

This distribution is unreliable. With only 8 points on a 2^64-sized ring, gap sizes between servers are determined by random chance. Some gaps end up several times larger than others, and whichever server sits at the end of the largest gap inherits a disproportionate amount of keys.

### Fix
Use virtual nodes. Instead of hashing each server once, hash it ~150 times using varied inputs (`serverName + strconv.Itoa(i)`) and register each of those hashes on the ring, all pointing to the same physical server. 

With 8 servers × 150 virtual nodes = 1,200 ring points, gap sizes even out and load distribution becomes better.