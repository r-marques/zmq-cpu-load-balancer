# Zmq CPU Load Balancer

This is an attempt at having a zmq load balancer that dynamically adjusts the
number of workers in order to keep the machine cpu load average below a certain
value (by default `0.8 * num_cores`)


##### Example

```bash
$ python3 balancer.py
# in another terminal
$ python3 client.py
```
