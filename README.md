# moosefs_prometheus
MooseFS exporter for Prometheus


# Installation :

### Prerequisites
```
sudo apt-get update
sudo apt-get install python3 python3-pip
```

### Install dependencies
`pip3 install prometheus_client`

### Download the exporter
```
wget https://raw.githubusercontent.com/liberodark/moosefs_prometheus/refs/heads/master/moosefs_exporter.py
chmod +x moosefs_exporter.py
```

# Usage :

### Basic usage
`python3 moosefs_exporter.py`

### Custom host and port
`python3 moosefs_exporter.py -H 192.168.1.100 -p 9900 -i 30`


# Prometheus Configuration :

Add to `prometheus.yml`

```
scrape_configs:
  - job_name: 'moosefs'
    static_configs:
      - targets: ['localhost:9841']
```