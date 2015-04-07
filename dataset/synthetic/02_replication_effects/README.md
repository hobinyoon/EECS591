# Synthetic Dataset #2

## Goal

The goal of this synthetic dataset is to demonstrate that replication can also reduce the latency between the user and the server. Algorithms that support replication should achieve better latency than the the algorithms that does not replicate the data (Volley).

## Dataset Details:

The dataset contains ten users, three servers. The two group of users are located on the opposite side of the world e.g. one in California and another one in China. The three servers are located in between the two groups of users where one server is approximately in the middle between two groups of users and the other two are close to the first and the second group respectively. The data is uploaded by the first user to the server closest to him.

### List of users:

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


### List of servers:

1. _Left server:_ 4.4.4.1 - (40.605612, -109.423828)
2. _Center server:_ 4.4.4.2 - (40.793019, -93.559570)
3. _Right server:_ 4.4.4.3 - (40.447781, -83.408203)
