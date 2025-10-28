#!/usr/bin/env python3
"""
LocalPulse Scheduler Manager

This script provides utilities to manage the Celery scheduler and workers.
"""

import os
import sys
import subprocess
import signal
import time
import argparse
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class SchedulerManager:
    """Manage LocalPulse Celery workers and scheduler"""
    
    def __init__(self):
        self.project_root = project_root
        self.scheduler_dir = self.project_root / "scheduler"
        
    def start_worker(self, queues=None, concurrency=2):
        """Start a Celery worker"""
        cmd = ["celery", "-A", "scheduler.celery_app", "worker"]
        
        if queues:
            cmd.extend(["--queues", queues])
        
        cmd.extend([
            "--concurrency", str(concurrency),
            "--loglevel", "info",
            "--pool", "solo" if sys.platform == "win32" else "prefork"
        ])
        
        print(f"Starting Celery worker with command: {' '.join(cmd)}")
        
        try:
            process = subprocess.Popen(
                cmd,
                cwd=self.project_root,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            
            print(f"Celery worker started with PID: {process.pid}")
            return process
            
        except Exception as e:
            print(f"Failed to start worker: {e}")
            return None
    
    def start_beat(self):
        """Start Celery Beat scheduler"""
        cmd = [
            "celery", "-A", "scheduler.celery_app", "beat",
            "--loglevel", "info"
        ]
        
        print(f"Starting Celery Beat with command: {' '.join(cmd)}")
        
        try:
            process = subprocess.Popen(
                cmd,
                cwd=self.project_root,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            
            print(f"Celery Beat started with PID: {process.pid}")
            return process
            
        except Exception as e:
            print(f"Failed to start beat: {e}")
            return None
    
    def start_flower(self, port=5555):
        """Start Flower monitoring web UI"""
        cmd = [
            "celery", "-A", "scheduler.celery_app", "flower",
            "--port", str(port)
        ]
        
        print(f"Starting Flower monitoring on port {port}")
        
        try:
            process = subprocess.Popen(
                cmd,
                cwd=self.project_root,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            
            print(f"Flower started with PID: {process.pid}")
            print(f"Access Flower at: http://localhost:{port}")
            return process
            
        except Exception as e:
            print(f"Failed to start Flower: {e}")
            return None
    
    def start_redis(self):
        """Start Redis server (if not already running)"""
        try:
            # Check if Redis is already running
            result = subprocess.run(
                ["redis-cli", "ping"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0 and "PONG" in result.stdout:
                print("Redis is already running")
                return None
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        # Try to start Redis
        try:
            print("Starting Redis server...")
            process = subprocess.Popen(
                ["redis-server"],
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            
            # Give Redis time to start
            time.sleep(2)
            
            # Verify Redis started
            result = subprocess.run(
                ["redis-cli", "ping"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                print(f"Redis started with PID: {process.pid}")
                return process
            else:
                print("Failed to verify Redis startup")
                return None
                
        except Exception as e:
            print(f"Failed to start Redis: {e}")
            print("Please install and start Redis manually")
            return None
    
    def stop_processes(self, processes):
        """Stop a list of processes"""
        for process in processes:
            if process and process.poll() is None:
                try:
                    if sys.platform == "win32":
                        process.send_signal(signal.CTRL_BREAK_EVENT)
                    else:
                        process.send_signal(signal.SIGTERM)
                    
                    # Wait for graceful shutdown
                    process.wait(timeout=10)
                    print(f"Process {process.pid} stopped gracefully")
                    
                except subprocess.TimeoutExpired:
                    # Force kill if graceful shutdown fails
                    process.kill()
                    print(f"Process {process.pid} force killed")
                    
                except Exception as e:
                    print(f"Error stopping process {process.pid}: {e}")
    
    def run_task(self, task_name, *args):
        """Run a specific task immediately"""
        try:
            from scheduler.tasks import app
            
            task_func = getattr(app, task_name, None)
            if not task_func:
                print(f"Task '{task_name}' not found")
                return False
            
            print(f"Running task: {task_name}")
            result = task_func.delay(*args)
            
            print(f"Task submitted with ID: {result.id}")
            
            # Wait for result (with timeout)
            try:
                task_result = result.get(timeout=300)  # 5 minutes timeout
                print(f"Task completed successfully: {task_result}")
                return True
                
            except Exception as e:
                print(f"Task failed: {e}")
                return False
                
        except Exception as e:
            print(f"Error running task: {e}")
            return False
    
    def show_status(self):
        """Show status of LocalPulse services"""
        print("LocalPulse Services Status:")
        print("-" * 30)
        
        # Check Redis
        try:
            result = subprocess.run(
                ["redis-cli", "ping"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0 and "PONG" in result.stdout:
                print("✅ Redis: Running")
            else:
                print("❌ Redis: Not responding")
                
        except Exception:
            print("❌ Redis: Not available")
        
        # Check MongoDB (basic check)
        try:
            from database.mongo_client import MongoDatabase
            db = MongoDatabase()
            db.connect()
            print("✅ MongoDB: Connected")
            db.close()
        except Exception:
            print("❌ MongoDB: Connection failed")
        
        # Check Celery workers (this would require more complex monitoring)
        print("ℹ️  Celery Workers: Use 'celery -A scheduler.celery_app inspect active' to check")
        print("ℹ️  Task Queue: Use Flower web UI for detailed monitoring")


def main():
    parser = argparse.ArgumentParser(description="LocalPulse Scheduler Manager")
    parser.add_argument("command", choices=[
        "start", "worker", "beat", "flower", "redis", "task", "status", "stop"
    ], help="Command to execute")
    
    parser.add_argument("--queues", help="Comma-separated list of queues for worker")
    parser.add_argument("--concurrency", type=int, default=2, help="Worker concurrency")
    parser.add_argument("--port", type=int, default=5555, help="Port for Flower")
    parser.add_argument("--task-name", help="Name of task to run")
    parser.add_argument("--task-args", nargs="*", help="Arguments for task")
    
    args = parser.parse_args()
    
    manager = SchedulerManager()
    processes = []
    
    try:
        if args.command == "start":
            # Start all services
            print("Starting LocalPulse Scheduler...")
            
            # Start Redis
            redis_process = manager.start_redis()
            if redis_process:
                processes.append(redis_process)
            
            time.sleep(2)  # Give Redis time to start
            
            # Start workers
            worker_process = manager.start_worker(
                queues=args.queues,
                concurrency=args.concurrency
            )
            if worker_process:
                processes.append(worker_process)
            
            # Start beat scheduler
            beat_process = manager.start_beat()
            if beat_process:
                processes.append(beat_process)
            
            print("All services started. Press Ctrl+C to stop.")
            
            # Wait for interrupt
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nShutting down...")
        
        elif args.command == "worker":
            worker_process = manager.start_worker(
                queues=args.queues,
                concurrency=args.concurrency
            )
            if worker_process:
                processes.append(worker_process)
                try:
                    worker_process.wait()
                except KeyboardInterrupt:
                    print("\nShutting down worker...")
        
        elif args.command == "beat":
            beat_process = manager.start_beat()
            if beat_process:
                processes.append(beat_process)
                try:
                    beat_process.wait()
                except KeyboardInterrupt:
                    print("\nShutting down beat...")
        
        elif args.command == "flower":
            flower_process = manager.start_flower(port=args.port)
            if flower_process:
                processes.append(flower_process)
                try:
                    flower_process.wait()
                except KeyboardInterrupt:
                    print("\nShutting down Flower...")
        
        elif args.command == "redis":
            redis_process = manager.start_redis()
            if redis_process:
                processes.append(redis_process)
                try:
                    redis_process.wait()
                except KeyboardInterrupt:
                    print("\nShutting down Redis...")
        
        elif args.command == "task":
            if not args.task_name:
                print("Task name is required. Use --task-name")
                return
            
            task_args = args.task_args or []
            manager.run_task(args.task_name, *task_args)
        
        elif args.command == "status":
            manager.show_status()
        
        elif args.command == "stop":
            print("Use Ctrl+C to stop running processes")
    
    finally:
        # Clean up processes
        if processes:
            print("Stopping processes...")
            manager.stop_processes(processes)


if __name__ == "__main__":
    main()