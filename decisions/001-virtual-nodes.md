## 001: Virtual Nodes and FNV Prefix Bias

**Goal:** Implement consistent hashing for key partitioning

**Naive implementation:**
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

**Run #2** (7 servers listed, one server did not receive any keys)
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

Worst: Run #2, 4800 / 400 = 12.0 (not including the dead server which would've been infinity)
Imbalance ratio: 12.0x
This distribution is unreliable. With only 8 points on a 2^64-sized ring, gap sizes between servers are determined by random chance. Some gaps end up several times larger than others, and whichever server sits at the end of the largest gap inherits a disproportionate amount of keys.

### Fix
Use virtual nodes. Instead of hashing each server once, hash it ~150 times using varied inputs and register each of those hashes on the ring, all pointing to the same physical server. 

With 8 servers × 150 virtual nodes = 1,200 ring points, gap sizes even out and load distribution becomes better.

### Experiment 1: With 150 virtual nodes & 10,000 keys

**Run 1:** 
```
09895c92-cbab-4947-86b5-3e0c41b96e6d: 1900 keys (19.0%)
6ee7cc28-7481-4d09-8dbe-d5367b734ece: 1700 keys (17.0%)
55ae42b7-3895-45b3-a29b-b95637f40315: 1500 keys (15.0%)
735411e6-fddf-466f-be12-da211139af8b: 1310 keys (13.1%)
94734b17-6b11-484a-8231-9697af5cf064: 1190 keys (11.9%)
6fc9e5f9-f11f-4e08-9b7a-a6aadeb53ea5: 1000 keys (10.0%)
2dae172a-c8aa-4b5b-ada1-f0780b790a34: 800 keys (8.0%)
78810104-8168-4dbc-b1ff-a625f2256952: 600 keys (6.0%)
```

**Run 2:**
```
cc0bcfcb-ea14-4c02-a877-fd6da0d54db5: 2490 keys(24.9%)
df5441ff-c06d-4263-99f1-42ce0a20dd1d: 1700 keys(17.0%)
e34a2271-6ee1-49e2-86a3-95bcf2e911b6: 1300 keys(13.0%)
3644edd7-3e81-4abc-8abd-a90437c29240: 1300 keys(13.0%)
b79d9966-8403-4790-9098-f2cfd17ac289: 1200 keys(12.0%)
a5078b91-1cba-4abe-84d6-6c30683e2a64: 900 keys(9.0%)
0410b476-dd5c-4457-b76e-3396a06d89de: 610 keys(6.1%)
07a5fea6-44d7-450c-b835-6094430c9245: 500 keys(5.0%)
```

**Run 3:**
```
be529a61-bfdb-42c3-af8c-c6edd8480d5a: 2100 keys(21.0%)
4f1ae6a0-14cb-4061-9eda-c93897a9adfa: 2100 keys(21.0%)
5d7edc70-f139-48d4-b8e9-e2f0036769ca: 1890 keys(18.9%)
20005e5b-e7e8-4659-87a9-50fbf714c92c: 1600 keys(16.0%)
a60b7e3c-a228-45d6-a9a0-f04be679e510: 910 keys(9.1%)
4aafd9f5-ce57-4d2f-b3bf-10b948742847: 550 keys(5.5%)
a1ccd9fe-e630-4f47-a1ed-3363f294af1f: 450 keys(4.5%)
53b94e5a-b2b3-4059-85bb-dc7dccb2d0dd: 400 keys(4.0%)
```

Worst: Run #3, 2100 / 400 = 5.25
Imbalance Ratio: 5.25x
Distribution improved over no vnodes (dead servers eliminated), but 
was still far from uniform


### Experiment 2: With 150 virtual nodes & 1,000,000 keys


**Run 1:**
```
c74c70f6-0ad2-4e54-9a50-5ff3cb4c622d: 207450 keys (20.745%)
96c11906-5fe5-4ccb-9015-6dd037e275f3: 186640 keys (18.664%)
58b217a1-bdc8-4913-903c-079d3116e366: 176935 keys (17.6935%)
64955e3e-e55f-471d-b4b0-9ac455604baa: 127250 keys (12.725%)
a79643b3-751a-4be4-b0fc-ea0b0e6a74b8: 112815 keys (11.2815%)
bab4b9b9-d56b-4a32-aaa8-205643845686: 76030 keys (7.603%)
444ba2e8-170e-4c48-82d1-2e454ea4ce12: 73840 keys (7.384%)
676c21e5-9d8e-4e99-abae-3fb3a227d096: 39040 keys (3.904%)
```

**Run 2:**
```
ce23fc16-c932-4943-a54a-818536ed0933: 205750 keys (20.575%)
10ee7d8e-7262-4644-ac5e-e5cb2509c83e: 152700 keys (15.270%)
fd8d1c51-f06f-40ab-8bb0-4c3620e0d6bc: 145393 keys (14.5393%)
4654c9b2-4706-407b-b08c-5bdc41c01075: 132452 keys (13.2452%)
e95d94c5-5c25-4297-84f5-0523b21ab047: 109292 keys (10.9292%)
8c991871-c326-4054-af45-9792bb1058ad: 96555 keys (9.6555%)
60faf2b5-4b5b-4b05-b828-150fb7dfc1a3: 90520 keys (9.052%)
76a13b51-ad3f-48da-aeb1-5252576ebb46: 67338 keys (6.7338%)
```

**Run 3:**
```
6066684d-36f9-4e1d-8c69-5ddbfad93725: 289620 keys (28.962%)
529fa55f-f907-4d2b-a87c-14df6547100b: 214750 keys (21.475%)
20019d68-0430-4754-9544-7b9402ff3404: 134850 keys (13.485%)
439fa9e6-6eca-4b90-b82d-38bce1792893: 102750 keys (10.275%)
d585c60e-8fcd-4d50-87c3-80241f6b5919: 87970 keys (8.797%)
df464658-bf38-43ab-8276-c03585a170e9: 74400 keys (7.440%)
da136693-c8df-432e-8e95-c5d886c948ca: 63800 keys (6.380%)
086d8f0a-78db-4d46-8edd-c6b33511d67c: 31860 keys (3.186%)

```
Worst: Run #3, 289620 / 31860 = 9.09
Imbalance Ratio: 9.09

Imbalanced distribution persisted even with 1M keys. The conclusion here is that sample size is not the main issue. The distribution is worse than the theoretical ideal. 

The hypothesis was that the hashing algorithm has a bias. Currently, we take the UUID generated from a string, and append a single, unique value at the end of it 150 times to create the vnodes. FNV-1a processes bytes left-to-right, XORing each byte into the state and multiplying by the FNV prime. When 150 inputs share a 36-byte prefix, the FNV state after 36 bytes is identical for all 150 inputs. Only the last 1-3 bytes differ, and those bytes get only one round of mixing each before the string ends.

To fix this, we changed the current construction of the vnodes from
```go
s + strconv.Itoa(int(i))
```
to 
```go
strconv.Itoa(int(i)) + s
```

### Experiment 3: With 150 virtual nodes & 1,000,000 keys & modification to hashing algorithm

**Run 1:**
```
f26911cd-bb4b-4fa0-a0fc-e45041e9146b: 138116 keys (13.8116%)
c7f24932-b7d1-437a-a3fd-6ed9ce88ad1d: 130721 keys (13.0721%)
2d6b48bf-fe89-49db-a3a6-13c949fc3bfc: 126031 keys (12.6031%)
8ebeaa70-378f-493e-b590-03aaf87d372c: 124764 keys (12.4764%)
f01ac420-bc50-4c5b-a7f1-d43e6f6e7402: 124385 keys (12.4385%)
e9525151-76c3-42f4-970c-e7ee8afa937b: 120773 keys (12.0773%)
7cfdf558-3678-45ba-9283-ef552e808280: 119091 keys (11.9091%)
32b84165-2a71-482d-9ee7-4ee5f2f27737: 116119 keys (11.6119%)
```

**Run 2:**
```
aca3c24c-86cd-436a-a255-460f50c2c1b7: 144432 keys (14.4432%)
dd79e2dc-19d1-4403-b775-4d0afff6b857: 135417 keys (13.5417%)
3963150d-d8f4-45dc-a0c5-7ba4f233c9b5: 125452 keys (12.5452%)
8d02551a-c059-444d-9bb0-c27c6037c49c: 125298 keys (12.5298%)
044564a2-39d2-413c-acec-5391da35cc6f: 119165 keys (11.9165%)
940fdf1a-4f1b-4f51-95dc-c94ac2d8f272: 118662 keys (11.8662%)
9889df4a-d341-47d8-bbc3-3aa092422119: 117002 keys (11.7002%)
7a03278a-3f91-4e22-8776-8ee834e6561f: 114572 keys (11.4572%)
```

**Run 3:**
```
ce5bc4b4-79a4-4b29-8ab6-42c3d5388f45: 130707 keys (13.0707%)
50b1bbc6-6c93-4df9-95f4-5e1bc597e733: 130507 keys (13.0507%)
80436071-6b63-4722-9651-07a45516839b: 127159 keys (12.7159%)
6e70a072-076d-4b2c-b746-296855b55fe7: 126595 keys (12.6595%)
5fcd10c9-a03c-49fc-ae1a-82285e940546: 125557 keys (12.5557%)
a2b9020d-70b9-4ac6-8757-ddf5fb3c1042: 123633 keys (12.3633%)
d4a5af92-b85c-4e64-8dd9-5fa27bc7ba3e: 122376 keys (12.2376%)
f658e2be-dabd-45ff-8dbe-60ef4d54653f: 113466 keys (11.3466%)
```

Worst: Run #2, 144432 / 114572 = 1.26
Imbalance ratio: 1.26x

### Conclusion
Every server takes roughly ±13% of the keys, distribution is now uniform. Virtual node quality depends not just on vnode count but on how virtual names are constructed. Non-cryptographic hashes are sensitive to shared prefixes. This is why production implementations often use hash 
constructions like `hash(i, hash(serverName))` or hash functions with 
better mixing (xxhash, murmur3) that are less prefix-sensitive.