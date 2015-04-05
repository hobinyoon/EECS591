# Sythetic Dataset #3

## Goal

We want to demostrate the effect of how the distributed algorithm can deal with change of workload better than the centralized algorithms. We want to use the SlashDot effect to demonstrate that the distributed algorithm can handle the workload better. Thus, it achieves a lower latency.

## Dataset Details

The first entry corresponds to the upload of the file from user 1 to his closest server, `4.4.4.1`. Then, 10 users access the same item at timestamp 2. There two groups of users and each group of users have a corresponding server. The reads happening at timestamp 2 should trigger the replication in the distributed algorithm. The set of access at timestamp 3 is for lowering the latency of the data.

_Assumption_: we assume that the centralized algorithms are not being executed until the end of the data. This is to demonstrate that the centralized algorithm cannot capture the change in workload.

### User List:

1. 5.5.5.1 - (43.667872, -116.367188)
2. 5.5.5.2 - (40.538852, -120.498047)
3. 5.5.5.3 - (47.420654, -122.343750)
4. 5.5.5.4 - (36.835668, -121.289063)
5. 5.5.5.5 - (44.925918, -116.894531)
6. 5.5.5.6 - (40.938415, -73.476563)
7. 5.5.5.7 - (38.160476, -79.365234)
8. 5.5.5.8 - (35.630512, -83.408203)
9. 5.5.5.9 - (41.795888, -70.751953)
10. 5.5.5.10 - (40.805494, -76.289063)

### Server List:

1. 4.4.4.1 - (40.605612, -109.423828)
2. 4.4.4.2 - (40.447781, -83.408203)
