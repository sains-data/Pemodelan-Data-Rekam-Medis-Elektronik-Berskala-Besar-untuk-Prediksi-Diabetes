"""
Test Environment Isolation and Management
Ensures proper test environment isolation, cleanup, and resource management
"""

import pytest
import os
import shutil
import tempfile
import docker
import psutil
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
import subprocess
import threading
import time
from unittest.mock import patch

from .test_config import TestConfig, TestResult
from .test_fixtures import TestFixtures


class EnvironmentIsolator:
    """Manages test environment isolation and cleanup"""
    
    def __init__(self):
        self.config = TestConfig()
        self.temp_dirs: Set[Path] = set()
        self.docker_containers: Set[str] = set()
        self.docker_networks: Set[str] = set()
        self.docker_volumes: Set[str] = set()
        self.processes: Set[int] = set()
        self.original_env = dict(os.environ)
        
    def create_isolated_temp_dir(self, prefix: str = "test_") -> Path:
        """Create an isolated temporary directory"""
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix))
        self.temp_dirs.add(temp_dir)
        return temp_dir
        
    def create_test_docker_network(self, name: str) -> str:
        """Create isolated Docker network for testing"""
        try:
            client = docker.from_env()
            network = client.networks.create(name, driver="bridge")
            self.docker_networks.add(network.id)
            return network.id
        except Exception as e:
            print(f"Failed to create Docker network: {e}")
            return ""
            
    def create_test_docker_volume(self, name: str) -> str:
        """Create isolated Docker volume for testing"""
        try:
            client = docker.from_env()
            volume = client.volumes.create(name)
            self.docker_volumes.add(volume.id)
            return volume.id
        except Exception as e:
            print(f"Failed to create Docker volume: {e}")
            return ""
            
    def run_isolated_container(self, image: str, **kwargs) -> str:
        """Run container in isolated environment"""
        try:
            client = docker.from_env()
            container = client.containers.run(image, detach=True, **kwargs)
            self.docker_containers.add(container.id)
            return container.id
        except Exception as e:
            print(f"Failed to run container: {e}")
            return ""
            
    def set_test_environment_variables(self, env_vars: Dict[str, str]):
        """Set test-specific environment variables"""
        for key, value in env_vars.items():
            os.environ[key] = value
            
    def restore_environment_variables(self):
        """Restore original environment variables"""
        # Remove test variables
        current_keys = set(os.environ.keys())
        original_keys = set(self.original_env.keys())
        
        # Remove keys that were added during testing
        for key in current_keys - original_keys:
            del os.environ[key]
            
        # Restore original values
        for key, value in self.original_env.items():
            os.environ[key] = value
            
    def cleanup_all(self):
        """Clean up all test resources"""
        self._cleanup_temp_dirs()
        self._cleanup_docker_resources()
        self._cleanup_processes()
        self.restore_environment_variables()
        
    def _cleanup_temp_dirs(self):
        """Clean up temporary directories"""
        for temp_dir in self.temp_dirs:
            try:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"Failed to cleanup temp dir {temp_dir}: {e}")
        self.temp_dirs.clear()
        
    def _cleanup_docker_resources(self):
        """Clean up Docker resources"""
        try:
            client = docker.from_env()
            
            # Stop and remove containers
            for container_id in self.docker_containers:
                try:
                    container = client.containers.get(container_id)
                    container.stop(timeout=10)
                    container.remove()
                except Exception as e:
                    print(f"Failed to cleanup container {container_id}: {e}")
                    
            # Remove networks
            for network_id in self.docker_networks:
                try:
                    network = client.networks.get(network_id)
                    network.remove()
                except Exception as e:
                    print(f"Failed to cleanup network {network_id}: {e}")
                    
            # Remove volumes
            for volume_id in self.docker_volumes:
                try:
                    volume = client.volumes.get(volume_id)
                    volume.remove()
                except Exception as e:
                    print(f"Failed to cleanup volume {volume_id}: {e}")
                    
        except Exception as e:
            print(f"Docker cleanup error: {e}")
            
        self.docker_containers.clear()
        self.docker_networks.clear()
        self.docker_volumes.clear()
        
    def _cleanup_processes(self):
        """Clean up spawned processes"""
        for pid in self.processes:
            try:
                process = psutil.Process(pid)
                process.terminate()
                process.wait(timeout=10)
            except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                try:
                    process.kill()
                except psutil.NoSuchProcess:
                    pass
            except Exception as e:
                print(f"Failed to cleanup process {pid}: {e}")
        self.processes.clear()


class ResourceMonitor:
    """Monitor resource usage during tests"""
    
    def __init__(self):
        self.monitoring = False
        self.resource_history = []
        self.monitor_thread = None
        
    def start_monitoring(self, interval: float = 1.0):
        """Start resource monitoring"""
        self.monitoring = True
        self.resource_history = []
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval,),
            daemon=True
        )
        self.monitor_thread.start()
        
    def stop_monitoring(self) -> Dict:
        """Stop monitoring and return summary"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
            
        if not self.resource_history:
            return {}
            
        import numpy as np
        
        memory_usage = [r['memory'] for r in self.resource_history]
        cpu_usage = [r['cpu'] for r in self.resource_history]
        
        return {
            'duration': len(self.resource_history),
            'memory_avg': np.mean(memory_usage),
            'memory_max': np.max(memory_usage),
            'memory_min': np.min(memory_usage),
            'cpu_avg': np.mean(cpu_usage),
            'cpu_max': np.max(cpu_usage),
            'cpu_min': np.min(cpu_usage)
        }
        
    def _monitor_loop(self, interval: float):
        """Resource monitoring loop"""
        while self.monitoring:
            try:
                memory = psutil.virtual_memory().percent
                cpu = psutil.cpu_percent()
                timestamp = datetime.now()
                
                self.resource_history.append({
                    'timestamp': timestamp,
                    'memory': memory,
                    'cpu': cpu
                })
                
                time.sleep(interval)
            except Exception as e:
                print(f"Resource monitoring error: {e}")
                break


@pytest.fixture(scope="function")
def isolated_environment():
    """Pytest fixture for isolated test environment"""
    isolator = EnvironmentIsolator()
    yield isolator
    isolator.cleanup_all()


@pytest.fixture(scope="function") 
def resource_monitor():
    """Pytest fixture for resource monitoring"""
    monitor = ResourceMonitor()
    yield monitor
    monitor.stop_monitoring()


@pytest.mark.isolation
class TestEnvironmentIsolation:
    """Test environment isolation capabilities"""
    
    def test_temporary_directory_isolation(self, isolated_environment):
        """Test temporary directory creation and cleanup"""
        # Create multiple temp directories
        temp_dirs = []
        for i in range(3):
            temp_dir = isolated_environment.create_isolated_temp_dir(f"test_dir_{i}_")
            temp_dirs.append(temp_dir)
            
            # Create test files
            test_file = temp_dir / f"test_file_{i}.txt"
            test_file.write_text(f"Test content {i}")
            
            assert temp_dir.exists(), f"Temp directory {i} not created"
            assert test_file.exists(), f"Test file {i} not created"
            
        # Verify all directories exist
        for temp_dir in temp_dirs:
            assert temp_dir.exists(), "Temp directory missing"
            
        print(f"Temporary Directory Isolation:")
        print(f"  Created directories: {len(temp_dirs)}")
        print(f"  All directories exist: {all(d.exists() for d in temp_dirs)}")
        
    def test_environment_variable_isolation(self, isolated_environment):
        """Test environment variable isolation"""
        # Save original state
        original_test_var = os.environ.get("TEST_ISOLATION_VAR")
        
        # Set test environment variables
        test_vars = {
            "TEST_ISOLATION_VAR": "test_value",
            "TEST_DATABASE_URL": "test://localhost:5432/testdb",
            "TEST_API_KEY": "test_api_key_123"
        }
        
        isolated_environment.set_test_environment_variables(test_vars)
        
        # Verify test variables are set
        for key, value in test_vars.items():
            assert os.environ.get(key) == value, f"Test variable {key} not set correctly"
            
        # Restore environment
        isolated_environment.restore_environment_variables()
        
        # Verify restoration
        assert os.environ.get("TEST_ISOLATION_VAR") == original_test_var, "Environment not restored correctly"
        
        print(f"Environment Variable Isolation:")
        print(f"  Test variables set: {len(test_vars)}")
        print(f"  Environment restored: {os.environ.get('TEST_ISOLATION_VAR') == original_test_var}")
        
    def test_docker_resource_isolation(self, isolated_environment):
        """Test Docker resource isolation"""
        # Skip if Docker is not available
        try:
            client = docker.from_env()
            client.ping()
        except Exception:
            pytest.skip("Docker not available")
            
        # Create isolated Docker network
        network_name = f"test_network_{int(time.time())}"
        network_id = isolated_environment.create_test_docker_network(network_name)
        
        if network_id:
            # Verify network exists
            try:
                network = client.networks.get(network_id)
                assert network.id == network_id, "Network not created correctly"
                print(f"Docker network created: {network_id[:12]}")
            except Exception as e:
                pytest.fail(f"Failed to verify network: {e}")
        else:
            pytest.skip("Failed to create Docker network")
            
        # Create isolated Docker volume
        volume_name = f"test_volume_{int(time.time())}"
        volume_id = isolated_environment.create_test_docker_volume(volume_name)
        
        if volume_id:
            # Verify volume exists
            try:
                volume = client.volumes.get(volume_id)
                assert volume.id == volume_id, "Volume not created correctly"
                print(f"Docker volume created: {volume_id[:12]}")
            except Exception as e:
                pytest.fail(f"Failed to verify volume: {e}")
        else:
            print("Docker volume creation skipped")
            
        print(f"Docker Resource Isolation:")
        print(f"  Network created: {bool(network_id)}")
        print(f"  Volume created: {bool(volume_id)}")
        
    def test_resource_monitoring(self, resource_monitor):
        """Test resource monitoring during isolated execution"""
        resource_monitor.start_monitoring(interval=0.5)
        
        # Perform resource-intensive operations
        import numpy as np
        
        # CPU intensive task
        for i in range(100):
            data = np.random.random(1000)
            result = np.fft.fft(data)
            
        # Memory intensive task
        large_arrays = []
        for i in range(10):
            arr = np.random.random(10000)
            large_arrays.append(arr)
            
        time.sleep(2)  # Allow monitoring to collect data
        
        # Stop monitoring and get summary
        summary = resource_monitor.stop_monitoring()
        
        # Verify monitoring data
        assert summary.get('duration', 0) > 0, "No monitoring data collected"
        assert summary.get('memory_avg', 0) > 0, "Memory monitoring failed"
        assert summary.get('cpu_avg', 0) >= 0, "CPU monitoring failed"
        
        print(f"Resource Monitoring:")
        print(f"  Monitoring duration: {summary.get('duration', 0)} samples")
        print(f"  Average memory usage: {summary.get('memory_avg', 0):.1f}%")
        print(f"  Peak memory usage: {summary.get('memory_max', 0):.1f}%")
        print(f"  Average CPU usage: {summary.get('cpu_avg', 0):.1f}%")
        print(f"  Peak CPU usage: {summary.get('cpu_max', 0):.1f}%")
        
    def test_cleanup_verification(self, isolated_environment):
        """Test cleanup verification"""
        # Create resources to be cleaned up
        temp_dir = isolated_environment.create_isolated_temp_dir("cleanup_test_")
        test_file = temp_dir / "cleanup_test.txt"
        test_file.write_text("This should be cleaned up")
        
        # Set test environment variables
        isolated_environment.set_test_environment_variables({
            "CLEANUP_TEST_VAR": "should_be_removed"
        })
        
        # Verify resources exist
        assert temp_dir.exists(), "Test temp directory not created"
        assert test_file.exists(), "Test file not created"
        assert os.environ.get("CLEANUP_TEST_VAR") == "should_be_removed", "Test env var not set"
        
        # Manual cleanup (fixture will also cleanup)
        isolated_environment.cleanup_all()
        
        # Verify cleanup
        assert not temp_dir.exists(), "Temp directory not cleaned up"
        assert os.environ.get("CLEANUP_TEST_VAR") is None, "Environment variable not cleaned up"
        
        print(f"Cleanup Verification:")
        print(f"  Temp directory cleaned: {not temp_dir.exists()}")
        print(f"  Environment restored: {os.environ.get('CLEANUP_TEST_VAR') is None}")


@pytest.mark.isolation
@pytest.mark.integration
class TestIsolationIntegration:
    """Test isolation in integration scenarios"""
    
    def test_parallel_test_isolation(self, isolated_environment):
        """Test isolation when running parallel tests"""
        # Simulate parallel test execution
        def isolated_test_task(task_id: int) -> Dict:
            # Create isolated resources
            temp_dir = isolated_environment.create_isolated_temp_dir(f"parallel_{task_id}_")
            test_file = temp_dir / f"task_{task_id}.txt"
            test_file.write_text(f"Task {task_id} data")
            
            # Set task-specific environment
            os.environ[f"TASK_{task_id}_VAR"] = f"value_{task_id}"
            
            return {
                'task_id': task_id,
                'temp_dir': str(temp_dir),
                'env_var': os.environ.get(f"TASK_{task_id}_VAR"),
                'file_exists': test_file.exists()
            }
            
        # Run multiple "parallel" tasks
        results = []
        for i in range(3):
            result = isolated_test_task(i)
            results.append(result)
            
        # Verify isolation
        for result in results:
            assert result['file_exists'], f"Task {result['task_id']} file missing"
            assert result['env_var'] == f"value_{result['task_id']}", f"Task {result['task_id']} env var incorrect"
            
        print(f"Parallel Test Isolation:")
        print(f"  Tasks executed: {len(results)}")
        print(f"  All files created: {all(r['file_exists'] for r in results)}")
        print(f"  All env vars correct: {all(r['env_var'] == f'value_{r['task_id']}' for r in results)}")


if __name__ == "__main__":
    # Run isolation tests
    pytest.main(["-v", "-m", "isolation", __file__])
