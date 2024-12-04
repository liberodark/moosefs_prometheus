#!/usr/bin/env python3
"""
MooseFS Prometheus Exporter

This script exports MooseFS metrics to Prometheus for monitoring and alerting.

Requirements:
- Python 3.7+
- prometheus_client
- MooseFS CLI tools

Usage:
- Install dependencies: pip install prometheus_client
- Run: python3 moosefs_exporter.py
"""

import subprocess
import time
import re
import logging
import argparse
import prometheus_client
from prometheus_client import Gauge, start_http_server
from typing import Optional, Tuple

class MooseFSExporter:
    def __init__(self, host: str = '100.66.36.111', timeout: int = 10):
        """
        Initialize MooseFS Prometheus Exporter
        
        :param host: MooseFS master host
        :param timeout: Command execution timeout in seconds
        """
        self.host = host
        self.timeout = timeout
        
        # System Metrics
        self.mfs_version = Gauge('moosefs_master_version', 'MooseFS master version')
        self.mfs_ram_used = Gauge('moosefs_ram_used_bytes', 'RAM used by master')
        self.mfs_cpu_total = Gauge('moosefs_cpu_usage_percent', 'Total CPU usage')
        self.mfs_cpu_system = Gauge('moosefs_cpu_system_percent', 'System CPU usage')
        self.mfs_cpu_user = Gauge('moosefs_cpu_user_percent', 'User CPU usage')
        
        # Space Metrics
        self.mfs_total_space = Gauge('moosefs_total_space_bytes', 'Total storage space')
        self.mfs_free_space = Gauge('moosefs_free_space_bytes', 'Free storage space')
        self.mfs_trash_space = Gauge('moosefs_trash_space_bytes', 'Trash space used')
        
        # Object Metrics
        self.mfs_total_objects = Gauge('moosefs_total_objects', 'Total filesystem objects')
        self.mfs_directories = Gauge('moosefs_directories', 'Number of directories')
        self.mfs_files = Gauge('moosefs_files', 'Number of files')
        self.mfs_chunks = Gauge('moosefs_chunks', 'Number of chunks')
        
        # Chunkserver Metrics
        self.mfs_chunkserver_chunks = Gauge(
            'moosefs_chunkserver_chunks', 
            'Number of chunks per chunkserver', 
            ['ip', 'server_id']
        )
        self.mfs_chunkserver_disk_used = Gauge(
            'moosefs_chunkserver_disk_used_bytes', 
            'Used disk space per chunkserver', 
            ['ip', 'server_id']
        )
        self.mfs_chunkserver_disk_total = Gauge(
            'moosefs_chunkserver_disk_total_bytes', 
            'Total disk space per chunkserver', 
            ['ip', 'server_id']
        )
        self.mfs_chunkserver_disk_usage_percent = Gauge(
            'moosefs_chunkserver_disk_usage_percent', 
            'Disk usage percentage per chunkserver', 
            ['ip', 'server_id']
        )
        
        # I/O Metrics
        self.mfs_chunkserver_read_speed = Gauge(
            'moosefs_chunkserver_read_speed_bytes_per_second', 
            'Read speed per chunkserver', 
            ['ip']
        )
        self.mfs_chunkserver_write_speed = Gauge(
            'moosefs_chunkserver_write_speed_bytes_per_second', 
            'Write speed per chunkserver', 
            ['ip']
        )
        
        # Logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _execute_command(self, command: str) -> Optional[str]:
        """
        Execute MooseFS CLI command with timeout
        
        :param command: Command to execute
        :return: Command output or None
        """
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                capture_output=True, 
                text=True, 
                timeout=self.timeout
            )
            return result.stdout if result.returncode == 0 else None
        except subprocess.TimeoutExpired:
            self.logger.error(f"Command timed out: {command}")
        except Exception as e:
            self.logger.error(f"Command execution error: {e}")
        return None

    def collect_system_metrics(self) -> bool:
        """
        Collect global MooseFS system metrics
        
        :return: Success status
        """
        try:
            output = self._execute_command(f'mfscli -H {self.host} -SIG')
            if not output:
                return False

            metrics_map = {
                'version': (r'master version\s+:\s+(\S+)', float, self.mfs_version),
                'ram_used': (r'RAM used\s+:\s+(\d+)\s*MiB', lambda x: int(x) * 1024 * 1024, self.mfs_ram_used),
                'cpu_total': (r'CPU used\s+:\s+(\d+\.\d+)%', float, self.mfs_cpu_total),
                'cpu_system': (r'CPU used \(system\)\s+:\s+(\d+\.\d+)%', float, self.mfs_cpu_system),
                'cpu_user': (r'CPU used \(user\)\s+:\s+(\d+\.\d+)%', float, self.mfs_cpu_user),
                'total_space': (r'total space\s+:\s+(\d+\.\d+)\s*TiB', lambda x: float(x) * 1024 * 1024 * 1024 * 1024, self.mfs_total_space),
                'free_space': (r'free space\s+:\s+(\d+\.\d+)\s*TiB', lambda x: float(x) * 1024 * 1024 * 1024 * 1024, self.mfs_free_space),
                'trash_space': (r'trash space\s+:\s+(\d+)\s*MiB', lambda x: int(x) * 1024 * 1024, self.mfs_trash_space),
                'total_objects': (r'all fs objects\s+:\s+(\d+)', int, self.mfs_total_objects),
                'directories': (r'directories\s+:\s+(\d+)', int, self.mfs_directories),
                'files': (r'files\s+:\s+(\d+)', int, self.mfs_files),
                'chunks': (r'chunks\s+:\s+(\d+)', int, self.mfs_chunks)
            }

            for metric_name, (pattern, converter, gauge) in metrics_map.items():
                match = re.search(pattern, output)
                if match:
                    try:
                        value = converter(match.group(1))
                        gauge.set(value)
                    except Exception as e:
                        self.logger.warning(f"Error processing {metric_name}: {e}")

            return True
        except Exception as e:
            self.logger.error(f"System metrics collection error: {e}")
            return False

    def collect_chunkserver_metrics(self) -> bool:
        """
        Collect MooseFS chunkserver metrics
        
        :return: Success status
        """
        try:
            output = self._execute_command(f'mfscli -H {self.host} -SCS')
            if not output:
                return False

            chunkserver_pattern = r'(\d+\.\d+\.\d+\.\d+)\s+\d+\s+(\d+)\s+[-]\s+\d+\.\d+\.\d+\s+\d+\s+\w+\s+\w+\s+(\d+)\s+(\d+\.\d+)\s*GiB\s+(\d+)\s*GiB\s+(\d+\.\d+)%'
            chunkserver_lines = re.findall(chunkserver_pattern, output)

            for line in chunkserver_lines:
                ip, server_id, chunks, used, total, used_percent = line
                
                # Convert values
                chunks = int(chunks)
                used_bytes = float(used) * 1024 * 1024 * 1024
                total_bytes = int(total) * 1024 * 1024 * 1024
                used_percent = float(used_percent)

                # Update chunkserver metrics
                labels = {'ip': ip, 'server_id': server_id}
                self.mfs_chunkserver_chunks.labels(**labels).set(chunks)
                self.mfs_chunkserver_disk_used.labels(**labels).set(used_bytes)
                self.mfs_chunkserver_disk_total.labels(**labels).set(total_bytes)
                self.mfs_chunkserver_disk_usage_percent.labels(**labels).set(used_percent)

            return True
        except Exception as e:
            self.logger.error(f"Chunkserver metrics collection error: {e}")
            return False

    def collect_io_metrics(self) -> bool:
        """
        Collect MooseFS I/O metrics
        
        :return: Success status
        """
        try:
            output = self._execute_command(f'mfscli -H {self.host} -SHD')
            if not output:
                return False

            io_pattern = r'(\d+\.\d+\.\d+\.\d+):\d+:.* (\d+\.\d+)\s*GiB/s\s+(\d+\.\d+)\s*MiB/s'
            io_lines = re.findall(io_pattern, output)

            for line in io_lines:
                ip, read_speed_gib, write_speed_mib = line
                
                # Convert speeds to bytes/second
                read_speed_bytes = float(read_speed_gib) * 1024 * 1024 * 1024
                write_speed_bytes = float(write_speed_mib) * 1024 * 1024
                
                # Update I/O metrics
                self.mfs_chunkserver_read_speed.labels(ip=ip).set(read_speed_bytes)
                self.mfs_chunkserver_write_speed.labels(ip=ip).set(write_speed_bytes)

            return True
        except Exception as e:
            self.logger.error(f"I/O metrics collection error: {e}")
            return False

    def collect_all_metrics(self) -> None:
        """
        Collect all MooseFS metrics
        """
        metrics_collectors = [
            self.collect_system_metrics,
            self.collect_chunkserver_metrics,
            self.collect_io_metrics
        ]

        for collector in metrics_collectors:
            try:
                collector()
            except Exception as e:
                self.logger.error(f"Metrics collection failed: {collector.__name__} - {e}")

    def run(self, port: int = 9841, interval: int = 15) -> None:
        """
        Run Prometheus metrics server
        
        :param port: Metrics server port
        :param interval: Metrics collection interval
        """
        self.logger.info(f"Starting MooseFS Prometheus Exporter on port {port}")
        start_http_server(port)
        
        while True:
            self.collect_all_metrics()
            time.sleep(interval)

def parse_arguments():
    """
    Parse command-line arguments
    
    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='MooseFS Prometheus Exporter')
    parser.add_argument(
        '-H', '--host', 
        default='100.66.36.111', 
        help='MooseFS master host'
    )
    parser.add_argument(
        '-p', '--port', 
        type=int, 
        default=9841, 
        help='Prometheus metrics port'
    )
    parser.add_argument(
        '-i', '--interval', 
        type=int, 
        default=15, 
        help='Metrics collection interval'
    )
    return parser.parse_args()

def main():
    """
    Main entry point
    """
    args = parse_arguments()
    
    exporter = MooseFSExporter(host=args.host)
    exporter.run(port=args.port, interval=args.interval)

if __name__ == '__main__':
    main()
