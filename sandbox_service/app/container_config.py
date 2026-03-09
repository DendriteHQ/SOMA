"""
Docker container configuration for sandbox execution.
"""

CONTAINER_CONFIG = {
    # Memory limit
    "mem_limit": "2g",
    
    # CPU limit (in nano CPUs, 1e9 = 1 CPU core)
    "nano_cpus": int(1e9),
    
    # Network isolation
    "network_mode": "none",
    
    # Run as nobody user (65534:65534)
    "user": "65534:65534",
    
    # Drop all capabilities for security
    "cap_drop": ["ALL"],
    
    # Make filesystem read-only
    "read_only": True,
    
    # Additional security options
    "security_opt": ["no-new-privileges:true"],
    
    # Limit number of processes
    "pids_limit": 256,
    
    # Temporary filesystem configuration
    "tmpfs": {
        "/tmp": "rw,noexec,nosuid,size=64m"
    },
}
