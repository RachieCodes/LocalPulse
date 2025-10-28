#!/usr/bin/env python3
"""
LocalPulse Setup Script

This script helps set up the LocalPulse environment and dependencies.
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

def run_command(command, check=True, shell=False):
    """Run a command and handle errors"""
    try:
        result = subprocess.run(
            command,
            check=check,
            shell=shell,
            capture_output=True,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(command) if isinstance(command, list) else command}")
        print(f"Error: {e.stderr}")
        return None

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("Error: Python 3.8 or higher is required")
        sys.exit(1)
    print(f"✅ Python {sys.version.split()[0]} detected")

def check_dependencies():
    """Check if required system dependencies are available"""
    print("Checking system dependencies...")
    
    # Check MongoDB
    try:
        mongo_result = run_command(["mongod", "--version"], check=False)
        if mongo_result and mongo_result.returncode == 0:
            print("✅ MongoDB is installed")
        else:
            print("❌ MongoDB not found or not responding")
            print_mongodb_install_instructions()
    except FileNotFoundError:
        print("❌ MongoDB not found in PATH")
        print_mongodb_install_instructions()
    
    # Check Redis
    try:
        redis_result = run_command(["redis-server", "--version"], check=False)
        if redis_result and redis_result.returncode == 0:
            print("✅ Redis is installed")
        else:
            print("❌ Redis not found or not responding")
            print_redis_install_instructions()
    except FileNotFoundError:
        print("❌ Redis not found in PATH")
        print_redis_install_instructions()

def print_mongodb_install_instructions():
    """Print MongoDB installation instructions"""
    if platform.system() == "Windows":
        print("   📥 Install MongoDB Community Edition:")
        print("   1. Download from: https://www.mongodb.com/try/download/community")
        print("   2. Run the installer and follow the setup wizard")
        print("   3. Add MongoDB to your PATH or use MongoDB Compass")
        print("   4. Start MongoDB service or run 'mongod' manually")
    else:
        print("   📥 Install MongoDB:")
        print("   - Ubuntu/Debian: sudo apt-get install mongodb")
        print("   - macOS: brew install mongodb-community")
        print("   - Or visit: https://docs.mongodb.com/manual/installation/")

def print_redis_install_instructions():
    """Print Redis installation instructions"""
    if platform.system() == "Windows":
        print("   📥 Install Redis for Windows:")
        print("   1. Download from: https://github.com/microsoftarchive/redis/releases")
        print("   2. Or use Windows Subsystem for Linux (WSL)")
        print("   3. Alternative: Use Redis Cloud (free tier available)")
        print("   4. For development: Use Docker - 'docker run -d -p 6379:6379 redis'")
    else:
        print("   📥 Install Redis:")
        print("   - Ubuntu/Debian: sudo apt-get install redis-server")
        print("   - macOS: brew install redis")
        print("   - Or visit: https://redis.io/download")

def install_python_packages():
    """Install Python dependencies"""
    print("Installing Python packages...")
    
    requirements_file = Path(__file__).parent / "requirements.txt"
    core_requirements_file = Path(__file__).parent / "requirements-core.txt"
    
    # Upgrade pip first
    pip_upgrade = run_command([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
    if not pip_upgrade:
        print("Warning: Failed to upgrade pip")
    
    # Try core requirements first
    if core_requirements_file.exists():
        print("Installing core packages first...")
        core_install = run_command([
            sys.executable, "-m", "pip", "install", "-r", str(core_requirements_file)
        ])
        
        if core_install:
            print("✅ Core packages installed successfully")
        else:
            print("⚠️  Some core packages failed to install")
    
    # Try full requirements
    if requirements_file.exists():
        print("Installing additional packages...")
        install_result = run_command([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ])
        
        if install_result:
            print("✅ All packages installed successfully")
            return True
        else:
            print("⚠️  Some additional packages failed to install")
            print("💡 You can try installing packages individually:")
            print("   pip install streamlit plotly pandas numpy")
            return False
    else:
        print("❌ requirements.txt not found")
        return False

def setup_environment():
    """Setup environment variables"""
    print("Setting up environment...")
    
    env_file = Path(__file__).parent / ".env"
    
    if env_file.exists():
        print("✅ .env file already exists")
        return
    
    env_content = """# LocalPulse Environment Configuration

# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DATABASE=localpulse

# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Scrapy Configuration
SCRAPY_PROJECT=localpulse

# Dashboard Configuration
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=localhost

# Logging
LOG_LEVEL=INFO
"""
    
    try:
        with open(env_file, 'w') as f:
            f.write(env_content)
        print("✅ Created .env file with default configuration")
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")

def download_nltk_data():
    """Download required NLTK data"""
    print("Downloading NLTK data...")
    
    try:
        import nltk
        
        # Download required datasets
        datasets = ['punkt', 'stopwords', 'vader_lexicon']
        
        for dataset in datasets:
            try:
                nltk.download(dataset, quiet=True)
                print(f"✅ Downloaded NLTK dataset: {dataset}")
            except Exception as e:
                print(f"❌ Failed to download {dataset}: {e}")
                
    except ImportError:
        print("⚠️  NLTK not installed yet. Will download after package installation.")

def create_directories():
    """Create necessary directories"""
    print("Creating directories...")
    
    base_path = Path(__file__).parent
    directories = [
        "logs",
        "data",
        "data/scraped",
        "data/processed",
        "static",
        "static/images"
    ]
    
    for directory in directories:
        dir_path = base_path / directory
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created directory: {directory}")
        except Exception as e:
            print(f"❌ Failed to create {directory}: {e}")

def test_connections():
    """Test database connections"""
    print("Testing connections...")
    
    # Test MongoDB
    try:
        from database.mongo_client import MongoDatabase
        db = MongoDatabase()
        db.connect()
        print("✅ MongoDB connection successful")
        db.close()
    except ImportError:
        print("⚠️  MongoDB client not available (pymongo not installed)")
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("   💡 Make sure MongoDB is running: mongod")
    
    # Test Redis
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✅ Redis connection successful")
    except ImportError:
        print("⚠️  Redis client not available (redis package not installed)")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("   💡 Make sure Redis is running: redis-server")

def setup_scrapy():
    """Initialize Scrapy project structure"""
    print("Setting up Scrapy...")
    
    scrapy_cfg_content = """[settings]
default = scrapers.settings

[deploy]
project = localpulse
"""
    
    try:
        with open("scrapy.cfg", 'w') as f:
            f.write(scrapy_cfg_content)
        print("✅ Created scrapy.cfg")
    except Exception as e:
        print(f"❌ Failed to create scrapy.cfg: {e}")

def create_startup_scripts():
    """Create convenient startup scripts"""
    print("Creating startup scripts...")
    
    # Dashboard startup script
    dashboard_script = """#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Start Streamlit dashboard
subprocess.run([
    sys.executable, "-m", "streamlit", "run", 
    str(project_root / "dashboard" / "main_dashboard.py"),
    "--server.port", "8501",
    "--server.address", "localhost"
])
"""
    
    # Scheduler startup script
    scheduler_script = """#!/usr/bin/env python3
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import and run scheduler manager
from scheduler.manager import main

if __name__ == "__main__":
    main()
"""
    
    try:
        with open("start_dashboard.py", 'w') as f:
            f.write(dashboard_script)
        print("✅ Created start_dashboard.py")
        
        with open("start_scheduler.py", 'w') as f:
            f.write(scheduler_script)
        print("✅ Created start_scheduler.py")
        
        # Make scripts executable on Unix systems
        if platform.system() != "Windows":
            os.chmod("start_dashboard.py", 0o755)
            os.chmod("start_scheduler.py", 0o755)
            
    except Exception as e:
        print(f"❌ Failed to create startup scripts: {e}")

def main():
    """Main setup function"""
    print("🚀 LocalPulse Setup")
    print("=" * 50)
    
    # Check Python version
    check_python_version()
    
    # Check system dependencies (non-blocking)
    check_dependencies()
    
    # Create directories
    create_directories()
    
    # Setup environment
    setup_environment()
    
    # Install Python packages
    if not install_python_packages():
        print("⚠️  Some packages failed to install, but continuing setup...")
    
    # Download NLTK data (after packages are installed)
    download_nltk_data()
    
    # Setup Scrapy
    setup_scrapy()
    
    # Create startup scripts
    create_startup_scripts()
    
    # Test connections (non-blocking)
    test_connections()
    
    print("\n" + "=" * 50)
    print("🎉 Setup Complete!")
    print("\n📋 Next steps:")
    print("1. Install MongoDB: https://www.mongodb.com/try/download/community")
    print("2. Install Redis: https://redis.io/download (or use Docker)")
    print("3. Start MongoDB: mongod")
    print("4. Start Redis: redis-server")
    print("5. Start dashboard: python start_dashboard.py")
    print("6. Start scheduler: python start_scheduler.py start")
    print("\n📖 For more information, see README.md")
    print("💡 Tip: You can run individual components even if some services are unavailable")

if __name__ == "__main__":
    main()