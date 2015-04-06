# Synthetic Dataset #1

## Goal

The goal of this synthetic dataset is to demonstrate that the algorithms are independent of the initial placement.

## Dataset Details:

The dataset contains ten users, two servers, and ten data. We can split the users into two groups based on his distance to the server. Each user uploads the data to his closest server. Each data item is accessed uniformly from users that are further away. For example, if the first data is uploaded to server 1, the data is being accessed from users from the other group, which are users that are closer to server 2. 

## Associated Code:

The script for generating the dataset is in `generate-data.py`.

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

1. 4.4.4.1 - (40.605612, -109.423828)
2. 4.4.4.2 - (40.793019, -93.559570)
