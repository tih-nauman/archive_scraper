#!/usr/bin/env python3
"""
High-Performance Torrent Downloader
Optimized for systems with high CPU cores and RAM
Supports both .torrent files and magnet links
"""

import asyncio
import aiofiles
import libtorrent as lt
import time
import sys
import os
import argparse
import logging
import psutil
import signal
from pathlib import Path
from typing import Optional, Dict, Any, List
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import json
from datetime import datetime

class HighPerformanceTorrentDownloader:
    def __init__(self, download_path: str = "./downloads", max_upload_rate: int = 0, max_download_rate: int = 0, 
                 zero_leech_mode: bool = False):
        """
        Initialize the torrent downloader with optimized settings
        
        Args:
            download_path: Directory to save downloaded files
            max_upload_rate: Maximum upload rate in KB/s (0 = unlimited)
            max_download_rate: Maximum download rate in KB/s (0 = unlimited)
            zero_leech_mode: Force zero-leech optimization mode (auto-detect if False)
        """
        self.download_path = Path(download_path)
        self.download_path.mkdir(exist_ok=True)
        
        # Setup logging first
        self.setup_logging()
        
        # Ensure logger is available
        if not hasattr(self, 'logger') or self.logger is None:
            self.logger = logging.getLogger(__name__)
            if not self.logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        
        # Zero-leech optimization mode
        self.force_zero_leech_mode = zero_leech_mode
        self.zero_leech_active = zero_leech_mode
        
        # Create libtorrent session with optimized settings
        self.session = lt.session()
        self.configure_session(max_upload_rate, max_download_rate)
        
        # Track active downloads
        self.active_torrents: Dict[str, lt.torrent_handle] = {}
        self.is_running = True
        
        # Speed tracking for better performance monitoring
        self.speed_history = {}  # Track speed history for each torrent
        self.last_update_time = time.time()
        self.session_start_time = time.time()
        
        # Performance optimization tracking
        self.performance_stats = {
            'total_downloaded': 0,
            'peak_download_speed': 0,
            'avg_download_speed': 0,
            'connection_attempts': 0,
            'successful_connections': 0
        }
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Queue management for download tasks
        self.download_queue: deque = deque()
        self.max_queue_size = 100000  # Maximum number of torrents in the queue
        self.max_concurrent_downloads = 30  # Maximum concurrent downloads
        
        # Download tracking
        self.download_stats = {
            'total_queued': 0,
            'total_completed': 0,
            'total_failed': 0,
            'total_downloaded_bytes': 0,
            'session_start': time.time()
        }
        
        # Track individual torrent progress
        self.torrent_progress = {}  # info_hash -> progress info
    
    def setup_logging(self):
        """Setup logging configuration"""
        try:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('torrent_downloader.log'),
                    logging.StreamHandler(sys.stdout)
                ]
            )
            self.logger = logging.getLogger(__name__)
        except Exception as e:
            # Fallback logger creation if basicConfig fails
            self.logger = logging.getLogger(__name__)
            self.logger.setLevel(logging.INFO)
            if not self.logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
    
    def configure_session(self, max_upload_rate: int, max_download_rate: int):
        """Configure libtorrent session for maximum performance"""
        # Configure session settings using available methods
        
        # Connection settings optimized for high-core systems
        self.session.set_max_connections(8000)
        self.session.set_max_half_open_connections(500)
        
        # Bandwidth settings
        if max_download_rate > 0:
            self.session.set_download_rate_limit(max_download_rate * 1024)
        if max_upload_rate > 0:
            self.session.set_upload_rate_limit(max_upload_rate * 1024)
        
        # Get current settings and modify them
        settings = self.session.get_settings()
        
        # Get system specs for optimal configuration
        cpu_cores = psutil.cpu_count()
        total_ram_gb = psutil.virtual_memory().total / (1024**3)
        
        # Determine optimization level based on zero-leech mode
        if self.zero_leech_active:
            # ULTRA-AGGRESSIVE settings for zero-leech scenarios
            cache_percentage = 0.6  # 60% of RAM
            connection_multiplier = 2.5
            thread_multiplier = 4
            buffer_multiplier = 2
            self.logger.info("🚨 ZERO-LEECH ULTRA MODE - Maximum Aggression!")
        else:
            # High-performance settings for normal scenarios
            cache_percentage = 0.4  # 40% of RAM
            connection_multiplier = 1.5
            thread_multiplier = 2
            buffer_multiplier = 1
        
        # Update performance-related settings with dynamic optimization
        performance_settings = {
            # Memory and cache settings - Dynamic based on mode
            'cache_size': int(total_ram_gb * cache_percentage * 1024),
            'cache_buffer_chunk_size': int(256 * buffer_multiplier),
            'use_read_cache': True,
            'use_write_cache': True,
            'guided_read_cache': True,
            'cache_expiry': 10800 if self.zero_leech_active else 7200,
            
            # Connection settings - Scaled based on mode
            'connections_limit': int(12000 * connection_multiplier),
            'connections_limit_global': int(15000 * connection_multiplier),
            'max_peerlist_size': int(15000 * connection_multiplier),
            'max_paused_peerlist_size': int(3000 * connection_multiplier),
            'allow_multiple_connections_per_ip': True,
            'max_peer_recv_buffer_size': int(1048576 * 4 * buffer_multiplier),
            
            # I/O settings for maximum performance
            'max_queued_disk_bytes': int(1048576 * 512 * buffer_multiplier),
            'send_buffer_watermark': int(1048576 * 20 * buffer_multiplier),
            'send_buffer_low_watermark': int(1048576 * 10 * buffer_multiplier),
            'send_buffer_watermark_factor': 300 if self.zero_leech_active else 200,
            'recv_socket_buffer_size': int(1048576 * 4 * buffer_multiplier),
            'send_socket_buffer_size': int(1048576 * 4 * buffer_multiplier),
            
            # Threading optimization - Scaled based on mode
            'aio_threads': min(int(cpu_cores * thread_multiplier), 512),
            'network_threads': min(cpu_cores if self.zero_leech_active else cpu_cores // 2, 64),
            'file_pool_size': 2000 if self.zero_leech_active else 1000,
            'disk_io_write_mode': 2 if self.zero_leech_active else 1,
            'disk_io_read_mode': 2 if self.zero_leech_active else 1,
            
            # Protocol settings optimized for speed
            'enable_incoming_tcp': True,
            'enable_outgoing_tcp': True,
            'enable_incoming_utp': True,
            'enable_outgoing_utp': True,
            'prefer_tcp': True,
            'utp_target_delay': 25 if self.zero_leech_active else 50,
            'utp_gain_factor': 5000 if self.zero_leech_active else 3000,
            
            # DHT optimization for better peer discovery
            'enable_dht': True,
            'dht_bootstrap_nodes': 'router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.utorrent.com:6881',
            'max_dht_items': 50000 if self.zero_leech_active else 10000,
            'dht_upload_rate_limit': 50000 if self.zero_leech_active else 8000,
            
            # Aggressive performance optimizations - Scaled by mode
            'piece_timeout': 5 if self.zero_leech_active else 15,
            'request_timeout': 10 if self.zero_leech_active else 30,
            'peer_timeout': 20 if self.zero_leech_active else 60,
            'inactivity_timeout': 120 if self.zero_leech_active else 300,
            'connection_speed': 3000 if self.zero_leech_active else 1000,
            'max_failcount': 5 if self.zero_leech_active else 2,
            'mixed_mode_algorithm': 0,
            'request_queue_time': 1 if self.zero_leech_active else 3,
            
            # Download-focused settings (minimal seeding)
            'share_ratio_limit': 0,
            'seed_time_ratio_limit': 0,
            'seed_time_limit': 0,
            'active_downloads': -1,
            'active_seeds': 0 if self.zero_leech_active else 3,
            'active_limit': -1,
            
            # Tracker optimization - More aggressive in zero-leech mode
            'min_announce_interval': 1 if self.zero_leech_active else 3,
            'tracker_completion_timeout': 10 if self.zero_leech_active else 20,
            'tracker_receive_timeout': 5 if self.zero_leech_active else 8,
            'max_http_recv_buffer_size': int(1048576 * 4 * buffer_multiplier),
            
            # Piece selection optimization - Different strategies per mode
            'piece_extent_affinity': 0 if self.zero_leech_active else 7,
            'suggest_mode': 2 if self.zero_leech_active else 1,
            'prioritize_partial_pieces': not self.zero_leech_active,
            
            # Memory optimization - More aggressive in zero-leech mode
            'max_suggest_pieces': 500 if self.zero_leech_active else 100,
            'max_allowed_in_request_queue': 5000 if self.zero_leech_active else 2000,
            'whole_pieces_threshold': 5 if self.zero_leech_active else 20,
            
            # File allocation for better disk performance
            'allocate_files': True,
            'no_atime_storage': True,
        }
        
        # Apply settings that exist (with compatibility checking)
        applied_settings = []
        skipped_settings = []
        
        for key, value in performance_settings.items():
            if key in settings:
                try:
                    settings[key] = value
                    applied_settings.append(key)
                except Exception as e:
                    skipped_settings.append(f"{key}: {e}")
            else:
                skipped_settings.append(f"{key}: not available in this libtorrent version")
        
        self.session.apply_settings(settings)
        
        if skipped_settings:
            self.logger.debug(f"Skipped {len(skipped_settings)} incompatible settings")
            # Only log first few to avoid spam
            for setting in skipped_settings[:3]:
                self.logger.debug(f"Skipped: {setting}")
        
        self.logger.info(f"Applied {len(applied_settings)} performance settings")
        
        # Set listen ports with multiple port ranges for better connectivity
        port_range = 40 if self.zero_leech_active else 20
        self.session.listen_on(6881, 6881 + port_range)
        
        # Add multiple DHT routers for better peer discovery
        dht_routers = [
            ("router.bittorrent.com", 6881),
            ("dht.transmissionbt.com", 6881),
            ("router.utorrent.com", 6881),
            ("dht.libtorrent.org", 25401),
        ]
        
        # Add extra DHT routers in zero-leech mode
        if self.zero_leech_active:
            dht_routers.extend([
                ("router.bitcomet.com", 554),
                ("dht.aelitis.com", 6881),
                ("tracker.opentrackr.org", 1337),
            ])
        
        try:
            for host, port in dht_routers:
                self.session.add_dht_router(host, port)
        except AttributeError:
            # DHT router method might not be available in this version
            pass
        
        # Enable Local Service Discovery
        try:
            self.session.start_lsd()
        except AttributeError:
            pass
        
        # Enable UPnP for better connectivity
        try:
            self.session.start_upnp()
            self.session.start_natpmp()
        except AttributeError:
            pass
        
        mode_name = "ZERO-LEECH ULTRA" if self.zero_leech_active else "HIGH-PERFORMANCE"
        self.logger.info(f"Session configured for {mode_name} mode")
        self.logger.info(f"Using {min(int(cpu_cores * thread_multiplier), 512)} I/O threads and {min(cpu_cores if self.zero_leech_active else cpu_cores // 2, 64)} network threads")
        self.logger.info(f"Cache size: {int(total_ram_gb * cache_percentage):.1f} GB ({cache_percentage * 100:.0f}% of RAM)")
        self.logger.info(f"Max connections: {int(12000 * connection_multiplier):,}")
        self.logger.info(f"Download limit: {max_download_rate} KB/s" if max_download_rate > 0 else "Download limit: UNLIMITED")
        self.logger.info(f"Upload limit: {max_upload_rate} KB/s" if max_upload_rate > 0 else "Upload limit: UNLIMITED")
        
        # Apply additional ultra-performance settings
        self._apply_ultra_performance_settings()
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.is_running = False
    
    def add_torrent_from_file(self, torrent_file_path: str, download_path: Optional[str] = None) -> str:
        """
        Add a torrent from a .torrent file with enhanced error handling
        
        Args:
            torrent_file_path: Path to the .torrent file
            download_path: Custom download path (optional)
            
        Returns:
            Torrent info hash as string
        """
        try:
            # Validate file exists and is readable
            if not os.path.isfile(torrent_file_path):
                raise FileNotFoundError(f"Torrent file not found: {torrent_file_path}")
            
            # Check file size (torrent files shouldn't be too small or too large)
            file_size = os.path.getsize(torrent_file_path)
            if file_size < 100:  # Too small to be a valid torrent
                raise ValueError(f"Torrent file too small ({file_size} bytes): {torrent_file_path}")
            if file_size > 50 * 1024 * 1024:  # Larger than 50MB is suspicious
                self.logger.warning(f"Large torrent file ({file_size/1024/1024:.1f} MB): {torrent_file_path}")
            
            # Try to read and validate the torrent file
            try:
                with open(torrent_file_path, 'rb') as f:
                    content = f.read(1024)  # Read first 1KB to check for basic validity
                    if not content.startswith(b'd'):  # Torrent files start with 'd' (bencoded dict)
                        raise ValueError(f"Invalid torrent file format: {torrent_file_path}")
            except Exception as e:
                raise ValueError(f"Cannot read torrent file {torrent_file_path}: {e}")
            
            # Create torrent info (with auto-repair if needed)
            try:
                torrent_info = lt.torrent_info(torrent_file_path)
            except Exception as e:
                # Enhanced error message for common issues
                error_msg = str(e).lower()
                if 'bdecode' in error_msg or 'invalid' in error_msg:
                    self.logger.warning(f"Torrent file appears corrupted, attempting repair...")
                    try:
                        repaired_path = self.repair_torrent_file(torrent_file_path)
                        torrent_info = lt.torrent_info(repaired_path)
                        self.logger.info(f"Successfully loaded repaired torrent file")
                        if repaired_path != torrent_file_path:
                            torrent_file_path = repaired_path  # Use repaired file
                    except Exception as repair_error:
                        raise ValueError(f"Corrupted torrent file that cannot be repaired: {torrent_file_path} ({e})")
                else:
                    raise ValueError(f"Failed to parse torrent file {torrent_file_path}: {e}")
            
            params = {
                'ti': torrent_info,
                'save_path': download_path or str(self.download_path),
                'flags': (
                    lt.torrent_flags.auto_managed |
                    lt.torrent_flags.duplicate_is_error |
                    lt.torrent_flags.apply_ip_filter |
                    lt.torrent_flags.sequential_download  # Enable sequential download for better disk I/O
                )
            }
            
            handle = self.session.add_torrent(params)
            
            # Apply download optimizations to the torrent
            self._optimize_torrent_for_download(handle)
            
            info_hash = str(torrent_info.info_hash())
            self.active_torrents[info_hash] = handle
            
            self.logger.info(f"Added torrent: {torrent_info.name()} ({file_size/1024:.1f} KB)")
            return info_hash
            
        except Exception as e:
            self.logger.error(f"Error adding torrent from file {torrent_file_path}: {e}")
            raise
    
    def add_torrent_from_magnet(self, magnet_link: str, download_path: Optional[str] = None) -> str:
        """
        Add a torrent from a magnet link
        
        Args:
            magnet_link: Magnet link URL
            download_path: Custom download path (optional)
            
        Returns:
            Torrent info hash as string
        """
        try:
            params = {
                'url': magnet_link,
                'save_path': download_path or str(self.download_path),
                'flags': (
                    lt.torrent_flags.auto_managed |
                    lt.torrent_flags.duplicate_is_error |
                    lt.torrent_flags.apply_ip_filter |
                    lt.torrent_flags.sequential_download  # Enable sequential download
                )
            }
            
            handle = self.session.add_torrent(params)
            
            # Wait for metadata with timeout
            self.logger.info("Waiting for metadata...")
            timeout = 60  # 60 second timeout
            start_time = time.time()
            
            while not handle.has_metadata():
                if time.time() - start_time > timeout:
                    self.logger.error("Metadata download timeout")
                    self.session.remove_torrent(handle)
                    raise TimeoutError("Failed to download metadata within 60 seconds")
                    
                time.sleep(0.1)
                if not self.is_running:
                    return ""
            
            # Apply download optimizations once metadata is available
            self._optimize_torrent_for_download(handle)
            
            info_hash = str(handle.info_hash())
            self.active_torrents[info_hash] = handle
            
            self.logger.info(f"Added magnet torrent: {handle.name()}")
            return info_hash
            
        except Exception as e:
            self.logger.error(f"Error adding magnet torrent {magnet_link}: {e}")
            raise
    
    def get_torrent_status(self, info_hash: str) -> Dict[str, Any]:
        """Get detailed status of a torrent with enhanced speed tracking"""
        if info_hash not in self.active_torrents:
            return {}
        
        try:
            handle = self.active_torrents[info_hash]
            
            # Check if handle is valid
            if not handle.is_valid():
                self.logger.warning(f"Invalid torrent handle for {info_hash}")
                return {}
            
            status = handle.status()
            
            # Enhanced speed calculation with proper units
            download_rate_kbs = status.download_rate / 1024 if status.download_rate > 0 else 0
            upload_rate_kbs = status.upload_rate / 1024 if status.upload_rate > 0 else 0
            
            # Get more detailed peer information
            peer_info = handle.get_peer_info() if hasattr(handle, 'get_peer_info') else []
            active_peers = len([p for p in peer_info if p.downloading_progress > 0]) if peer_info else status.num_peers
            
            return {
                'name': handle.name() if handle.has_metadata() else f"Torrent {info_hash[:8]}...",
                'progress': status.progress * 100,
                'download_rate': download_rate_kbs,
                'upload_rate': upload_rate_kbs,
                'download_rate_bytes': status.download_rate,
                'upload_rate_bytes': status.upload_rate,
                'num_peers': status.num_peers,
                'num_seeds': status.num_seeds,
                'active_peers': active_peers,
                'total_size': status.total_wanted,
                'downloaded': status.total_wanted_done,
                'remaining': status.total_wanted - status.total_wanted_done,
                'state': str(status.state),
                'eta': self._calculate_eta(status),
                'all_time_download': status.all_time_download,
                'session_download': status.total_download,
                'has_metadata': handle.has_metadata(),
                'queue_position': status.queue_position,
                'distributed_copies': status.distributed_copies,
            }
        except Exception as e:
            self.logger.error(f"Error getting status for torrent {info_hash}: {e}")
            return {}
    
    def _calculate_eta(self, status) -> str:
        """Calculate estimated time of arrival"""
        if status.download_rate <= 0:
            return "∞"
        
        remaining = status.total_wanted - status.total_wanted_done
        eta_seconds = remaining / status.download_rate
        
        if eta_seconds < 60:
            return f"{eta_seconds:.0f}s"
        elif eta_seconds < 3600:
            return f"{eta_seconds/60:.1f}m"
        else:
            return f"{eta_seconds/3600:.1f}h"
    
    def print_status(self, show_speed_graph: bool = False):
        """Print enhanced status of all active torrents with better speed display"""
        if not self.active_torrents:
            print("No active torrents")
            return
        
        # Get total speeds across all torrents
        total_download_speed = 0
        total_upload_speed = 0
        total_peers = 0
        total_seeds = 0
        
        print(f"\n{'='*120}")
        print(f"{'Name':<35} {'Progress':<10} {'Download':<15} {'Upload':<12} {'Size':<10} {'Peers':<8} {'Seeds':<8} {'ETA':<10} {'State':<12}")
        print(f"{'='*120}")
        
        for info_hash in list(self.active_torrents.keys()):
            status = self.get_torrent_status(info_hash)
            if status:
                name = status['name'][:34]
                progress_str = f"{status['progress']:.1f}%"
                
                # Enhanced speed display with proper formatting
                download_rate = status['download_rate']
                upload_rate = status['upload_rate']
                
                if download_rate >= 1024:  # Show in MB/s if >= 1024 KB/s
                    download_str = f"{download_rate/1024:.1f} MB/s"
                else:
                    download_str = f"{download_rate:.1f} KB/s"
                
                if upload_rate >= 1024:
                    upload_str = f"{upload_rate/1024:.1f} MB/s"
                else:
                    upload_str = f"{upload_rate:.1f} KB/s"
                
                # Size formatting
                total_size_mb = status['total_size'] / (1024*1024)
                if total_size_mb >= 1024:
                    size_str = f"{total_size_mb/1024:.1f} GB"
                else:
                    size_str = f"{total_size_mb:.1f} MB"
                
                peers_str = f"{status['active_peers']}/{status['num_peers']}"
                seeds_str = str(status['num_seeds'])
                eta_str = str(status['eta'])
                state_str = status['state'][:11]
                
                # Add to totals
                total_download_speed += download_rate
                total_upload_speed += upload_rate
                total_peers += status['num_peers']
                total_seeds += status['num_seeds']
                
                print(f"{name:<35} {progress_str:<10} {download_str:<15} {upload_str:<12} "
                      f"{size_str:<10} {peers_str:<8} {seeds_str:<8} {eta_str:<10} {state_str:<12}")
                
                # Show speed graph if requested
                if show_speed_graph:
                    graph = self.display_speed_graph(info_hash, 40)
                    print(f"    {graph}")
        
        print(f"{'='*120}")
        
        # Show totals with enhanced formatting
        if total_download_speed >= 1024:
            total_down_str = f"{total_download_speed/1024:.1f} MB/s"
        else:
            total_down_str = f"{total_download_speed:.1f} KB/s"
            
        if total_upload_speed >= 1024:
            total_up_str = f"{total_upload_speed/1024:.1f} MB/s"
        else:
            total_up_str = f"{total_upload_speed:.1f} KB/s"
        
        print(f"TOTALS: Download: {total_down_str} | Upload: {total_up_str} | "
              f"Peers: {total_peers} | Seeds: {total_seeds} | Active: {len(self.active_torrents)}")
        print(f"{'='*120}")
    
    async def monitor_downloads(self, update_interval: float = 2.0, show_speed_graph: bool = False):
        """Monitor download progress asynchronously with enhanced performance tracking"""
        boost_counter = 0
        zero_leech_mode_active = False
        stall_detection = {}  # Track stalled torrents
        performance_check_counter = 0
        
        while self.is_running and (self.active_torrents or self.download_queue):
            current_time = time.time()
            
            # Process queue first - start new downloads if slots available
            self.process_download_queue()
            
            # Enhanced status display with optional speed graphs
            self.print_status(show_speed_graph)
            
            # Print download summary every 5 iterations
            if performance_check_counter % 5 == 0:
                self.print_download_summary()
            
            # Track speeds for all torrents and update progress
            for info_hash in list(self.active_torrents.keys()):
                status = self.get_torrent_status(info_hash)
                if status:
                    self.track_speed_history(info_hash, status['download_rate'])
                    self.update_torrent_progress(info_hash, status)
            
            # Check for zero-leech scenario
            if not zero_leech_mode_active and self.detect_zero_leech_scenario():
                self.activate_zero_leech_mode()
                zero_leech_mode_active = True
            
            # Every 10 iterations (20 seconds), boost slow torrents
            boost_counter += 1
            if boost_counter >= 10:
                if zero_leech_mode_active:
                    # Special handling for zero-leech
                    for info_hash, handle in self.active_torrents.items():
                        status = handle.status()
                        if status.download_rate < 1024:  # Less than 1 KB/s
                            self.handle_zero_leech_stall(handle)
                else:
                    self.boost_slow_torrents()
                
                boost_counter = 0
            
            # Check for completed downloads
            completed = []
            for info_hash, handle in self.active_torrents.items():
                status = handle.status()
                if status.is_seeding or status.progress >= 1.0:
                    completed.append(info_hash)
                    self.logger.info(f"✅ Download completed: {handle.name()}")
                    
                    # Update stats
                    if info_hash in self.torrent_progress:
                        progress_info = self.torrent_progress[info_hash]
                        progress_info['status'] = 'completed'
                        progress_info['completion_time'] = time.time()
                        self.download_stats['total_downloaded_bytes'] += progress_info.get('downloaded_bytes', 0)
                    
                    self.download_stats['total_completed'] += 1
            
            # Remove completed torrents and clean up
            for info_hash in completed:
                if info_hash in self.active_torrents:
                    del self.active_torrents[info_hash]
                if info_hash in stall_detection:
                    del stall_detection[info_hash]
                if info_hash in self.speed_history:
                    del self.speed_history[info_hash]
            
            # Save progress every 10 iterations
            if performance_check_counter % 10 == 0:
                self.save_progress_to_file()
            
            # Show enhanced system performance every 5 iterations
            performance_check_counter += 1
            if performance_check_counter % 5 == 0:
                perf_stats = self.get_system_performance_stats()
                insights = self.get_performance_insights()
                mode_indicator = "🚨 ZERO-LEECH MODE" if zero_leech_mode_active else "NORMAL MODE"
                
                self.logger.info(f"System: CPU {perf_stats['cpu_usage']:.1f}%, "
                               f"RAM {perf_stats['memory_percent']:.1f}%, "
                               f"Disk {perf_stats['disk_used_percent']:.1f}% | {mode_indicator}")
                
                # Show performance insights
                if insights['peak_speed_mbps'] > 0:
                    self.logger.info(f"Performance: Peak {insights['peak_speed_mbps']:.1f} MB/s | "
                                   f"Session: {insights['total_session_time']/60:.1f}m")
                
                # Show recommendations if any
                for recommendation in insights['recommendations']:
                    self.logger.warning(recommendation)
                
                performance_check_counter = 0
            
            # Update timing
            self.last_update_time = current_time
            await asyncio.sleep(update_interval)
        
        self.logger.info("All downloads completed or stopped")
        self.save_progress_to_file()  # Final save
    
    def download_torrent(self, torrent_input: str, download_path: Optional[str] = None) -> str:
        """
        Download a torrent from file or magnet link
        
        Args:
            torrent_input: Path to .torrent file or magnet link
            download_path: Custom download path (optional)
            
        Returns:
            Torrent info hash
        """
        if torrent_input.startswith('magnet:'):
            return self.add_torrent_from_magnet(torrent_input, download_path)
        elif os.path.isfile(torrent_input) and torrent_input.endswith('.torrent'):
            return self.add_torrent_from_file(torrent_input, download_path)
        else:
            raise ValueError("Invalid torrent input. Provide a .torrent file path or magnet link")
    
    async def run_download(self, torrent_input: str, download_path: Optional[str] = None, update_interval: float = 2.0, show_speed_graph: bool = False):
        """Run the download process"""
        try:
            info_hash = self.download_torrent(torrent_input, download_path)
            self.logger.info(f"Started download with hash: {info_hash}")
            
            # Start monitoring
            await self.monitor_downloads(update_interval, show_speed_graph)
            
        except Exception as e:
            self.logger.error(f"Error during download: {e}")
            raise
        finally:
            self.cleanup()
    
    async def run_multiple_downloads(self, torrent_list: List[str], download_path: Optional[str] = None, 
                                   update_interval: float = 2.0, show_speed_graph: bool = False):
        """Run multiple downloads using the queue system"""
        try:
            # Load previous progress if available
            self.load_progress_from_file()
            
            # Add all torrents to queue
            added_count = self.add_multiple_torrents_to_queue(torrent_list, download_path)
            
            if added_count == 0:
                self.logger.error("No torrents were added to the queue")
                return
            
            self.logger.info(f"Starting queue processing with {added_count} torrents")
            self.logger.info(f"Max concurrent downloads: {self.max_concurrent_downloads}")
            
            # Start monitoring which will also process the queue
            await self.monitor_downloads(update_interval, show_speed_graph)
            
        except Exception as e:
            self.logger.error(f"Error during multiple downloads: {e}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        self.logger.info("Cleaning up...")
        for handle in self.active_torrents.values():
            handle.pause()
        self.active_torrents.clear()
    
    def _optimize_torrent_for_download(self, handle):
        """Apply aggressive optimizations to a torrent for maximum download speed"""
        try:
            # Set download priority to maximum for all files
            if handle.has_metadata():
                torrent_info = handle.torrent_file()
                num_files = torrent_info.num_files()
                
                # Set all files to highest priority
                priorities = [7] * num_files  # 7 = highest priority
                handle.prioritize_files(priorities)
                
                # Set piece priorities based on mode
                num_pieces = torrent_info.num_pieces()
                piece_priorities = []
                
                if self.zero_leech_active:
                    # Zero-leech mode: randomized priorities for flexibility
                    import random
                    for i in range(num_pieces):
                        if i < 5 or i >= num_pieces - 5:  # First/last 5 pieces
                            piece_priorities.append(7)  # Highest priority
                        else:
                            piece_priorities.append(random.randint(3, 6))  # Random medium-high
                else:
                    # Normal mode: sequential priority
                    for i in range(num_pieces):
                        if i < 10 or i >= num_pieces - 10:  # First/last 10 pieces
                            piece_priorities.append(7)  # Highest priority
                        else:
                            piece_priorities.append(4)  # Normal priority
                
                handle.prioritize_pieces(piece_priorities)
            
            # Set connection limits based on mode
            max_connections = 2000 if self.zero_leech_active else 500
            max_uploads = 0 if self.zero_leech_active else 3
            
            handle.set_max_connections(max_connections)
            handle.set_max_uploads(max_uploads)
            
            # Set sequential download based on mode
            handle.set_sequential_download(not self.zero_leech_active)
            
        except Exception as e:
            self.logger.warning(f"Could not apply all optimizations: {e}")
    
    def add_multiple_torrents(self, torrent_list: list, download_path: Optional[str] = None) -> list:
        """
        Add multiple torrents concurrently for maximum efficiency
        
        Args:
            torrent_list: List of torrent files or magnet links
            download_path: Custom download path (optional)
            
        Returns:
            List of torrent info hashes
        """
        info_hashes = []
        
        for torrent_input in torrent_list:
            try:
                if torrent_input.startswith('magnet:'):
                    info_hash = self.add_torrent_from_magnet(torrent_input, download_path)
                elif os.path.isfile(torrent_input) and torrent_input.endswith('.torrent'):
                    info_hash = self.add_torrent_from_file(torrent_input, download_path)
                else:
                    self.logger.error(f"Invalid torrent input: {torrent_input}")
                    continue
                    
                info_hashes.append(info_hash)
                
            except Exception as e:
                self.logger.error(f"Failed to add torrent {torrent_input}: {e}")
                continue
        
        self.logger.info(f"Added {len(info_hashes)} torrents successfully")
        return info_hashes
    
    def boost_slow_torrents(self):
        """Enhanced dynamic boost for slow torrents with intelligent resource allocation"""
        if not self.active_torrents:
            return
            
        try:
            torrent_speeds = []
            
            # Analyze current download speeds with more detail
            for info_hash, handle in self.active_torrents.items():
                status = self.get_torrent_status(info_hash)
                if status:
                    speed = status['download_rate']
                    progress = status['progress']
                    peers = status['num_peers']
                    seeds = status['num_seeds']
                    torrent_speeds.append((info_hash, handle, speed, progress, peers, seeds))
            
            # Sort by download speed (slowest first)
            torrent_speeds.sort(key=lambda x: x[2])
            
            # Intelligent boosting strategy
            for i, (info_hash, handle, speed, progress, peers, seeds) in enumerate(torrent_speeds):
                if i < len(torrent_speeds) // 2:  # Bottom half get boost
                    # Calculate boost level based on multiple factors
                    if speed < 10 and seeds > 0:  # Very slow but seeders available
                        self.apply_emergency_boost(handle)
                    elif speed < 100:  # Moderately slow
                        handle.set_max_connections(min(handle.max_connections() + 300, 1500))
                        handle.force_reannounce()
                    
                    # Additional boost for torrents with good seed/peer ratio
                    if seeds > peers and peers > 0:
                        handle.set_max_connections(min(handle.max_connections() + 200, 2000))
                
                elif i > len(torrent_speeds) * 0.75:  # Top quartile - maintain performance
                    # Keep fast torrents optimized but don't over-allocate
                    current_max = handle.max_connections()
                    if current_max > 1000:
                        handle.set_max_connections(max(current_max - 100, 500))
            
            # Apply dynamic connection optimization
            self.dynamic_connection_optimization()
            
            avg_speed = sum(x[2] for x in torrent_speeds) / len(torrent_speeds) if torrent_speeds else 0
            self.logger.info(f"Boost applied: {len(torrent_speeds)} torrents, avg speed: {avg_speed:.1f} KB/s")
            
        except Exception as e:
            self.logger.error(f"Error in boost_slow_torrents: {e}")
    
    def dynamic_connection_optimization(self):
        """Dynamically optimize connections based on current performance"""
        if not self.active_torrents:
            return
            
        try:
            total_speed = 0
            slow_torrents = []
            fast_torrents = []
            
            # Analyze current speeds
            for info_hash, handle in self.active_torrents.items():
                status = self.get_torrent_status(info_hash)
                if status:
                    speed = status['download_rate']
                    total_speed += speed
                    
                    if speed < 50:  # Less than 50 KB/s is considered slow
                        slow_torrents.append((handle, speed))
                    elif speed > 500:  # More than 500 KB/s is considered fast
                        fast_torrents.append((handle, speed))
            
            # Reallocate connections from fast to slow torrents
            if slow_torrents and fast_torrents:
                for handle, speed in slow_torrents:
                    # Boost slow torrents
                    current_max = handle.max_connections()
                    new_max = min(current_max + 200, 2000)
                    handle.set_max_connections(new_max)
                    
                    # Force more aggressive peer discovery
                    handle.force_reannounce()
                    if hasattr(handle, 'scrape_tracker'):
                        handle.scrape_tracker()
                
                # Slightly reduce fast torrents to balance resources
                for handle, speed in fast_torrents[:len(fast_torrents)//2]:
                    current_max = handle.max_connections()
                    new_max = max(current_max - 50, 200)
                    handle.set_max_connections(new_max)
            
            self.logger.info(f"Dynamic optimization: {len(slow_torrents)} slow, {len(fast_torrents)} fast torrents")
            
        except Exception as e:
            self.logger.warning(f"Dynamic connection optimization failed: {e}")
    
    def apply_emergency_boost(self, handle):
        """Apply emergency performance boost to a stalled torrent"""
        try:
            # Emergency settings for stalled downloads
            handle.set_max_connections(3000)  # Maximum connections
            handle.set_max_uploads(0)  # No uploads
            
            # Force all tracker announces
            handle.force_reannounce()
            if hasattr(handle, 'scrape_tracker'):
                handle.scrape_tracker()
            
            # If metadata available, prioritize differently
            if handle.has_metadata():
                torrent_info = handle.torrent_file()
                num_pieces = torrent_info.num_pieces()
                
                # Create emergency piece priority (focus on middle pieces)
                import random
                priorities = []
                for i in range(num_pieces):
                    if i < num_pieces // 4 or i > 3 * num_pieces // 4:
                        priorities.append(7)  # High priority for ends
                    else:
                        priorities.append(random.randint(5, 7))  # Random high for middle
                
                handle.prioritize_pieces(priorities)
            
            self.logger.warning(f"🚨 Emergency boost applied to {handle.name()}")
            
        except Exception as e:
            self.logger.error(f"Emergency boost failed: {e}")
    
    def get_system_performance_stats(self) -> Dict[str, Any]:
        """Get real-time system performance statistics"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage(str(self.download_path))
        
        # Network I/O
        net_io = psutil.net_io_counters()
        
        return {
            'cpu_usage': cpu_percent,
            'memory_used_gb': (memory.total - memory.available) / (1024**3),
            'memory_available_gb': memory.available / (1024**3),
            'memory_percent': memory.percent,
            'disk_free_gb': disk.free / (1024**3),
            'disk_used_percent': (disk.used / disk.total) * 100,
            'network_bytes_sent': net_io.bytes_sent,
            'network_bytes_recv': net_io.bytes_recv,
            'active_torrents': len(self.active_torrents)
        }
    
    def detect_zero_leech_scenario(self) -> bool:
        """Detect if we're in a zero-leech scenario (only seeders available)"""
        total_leechers = 0
        total_seeders = 0
        
        for handle in self.active_torrents.values():
            status = handle.status()
            total_leechers += status.num_incomplete
            total_seeders += status.num_complete
        
        # Zero leechers but seeders available
        return total_leechers == 0 and total_seeders > 0
    
    def activate_zero_leech_mode(self):
        """Activate special optimizations for zero-leech scenarios"""
        
        if self.zero_leech_active:
            return  # Already active
        
        self.logger.info("🚨 ZERO-LEECH DETECTED - Activating ultra mode!")
        self.zero_leech_active = True
        
        # Get current settings and modify for zero-leech
        settings = self.session.get_settings()
        
        # Get system specs
        total_ram_gb = psutil.virtual_memory().total / (1024**3)
        cpu_cores = psutil.cpu_count()
        
        # Ultra-aggressive settings for seeder competition
        zero_leech_boost = {
            # Increase cache to 60% of RAM
            'cache_size': int(total_ram_gb * 0.6 * 1024),
            
            # Maximum connections to compete for seeders
            'connections_limit': 30000,
            'connections_limit_global': 35000,
            'max_peerlist_size': 35000,
            
            # Ultra-fast timeouts - don't wait for slow seeders
            'piece_timeout': 3,
            'request_timeout': 8,
            'peer_timeout': 15,
            'connection_speed': 5000,
            
            # More aggressive DHT
            'max_dht_items': 100000,
            'dht_upload_rate_limit': 100000,
            
            # Disable sequential download - be flexible with piece order
            'strict_super_seeding': False,
            'piece_extent_affinity': 0,
            
            # Maximum request queue
            'max_allowed_in_request_queue': 10000,
            'max_suggest_pieces': 1000,
            
            # Zero upload bandwidth allocation
            'active_seeds': 0,
            'seed_time_limit': 0,
            'share_ratio_limit': 0,
            
            # More aggressive threading
            'aio_threads': min(cpu_cores * 4, 512),
            'network_threads': min(cpu_cores, 64),
            
            # Faster announces
            'min_announce_interval': 1,
            'tracker_completion_timeout': 5,
            'tracker_receive_timeout': 3,
        }
        
        # Apply zero-leech settings
        for key, value in zero_leech_boost.items():
            if key in settings:
                settings[key] = value
        
        self.session.apply_settings(settings)
        
        # Apply to all active torrents
        for handle in self.active_torrents.values():
            self._apply_zero_leech_torrent_settings(handle)
        
        self.logger.info("✅ Zero-leech ULTRA mode activated!")
        self.logger.info(f"🚀 Cache boosted to {int(total_ram_gb * 0.6):.1f} GB (60% RAM)")
        self.logger.info(f"🔗 Connections boosted to 30,000 per torrent")
        self.logger.info(f"⚡ I/O threads boosted to {min(cpu_cores * 4, 512)}")
    
    def _apply_zero_leech_torrent_settings(self, handle):
        """Apply zero-leech specific settings to a torrent"""
        
        # Maximum connections per torrent
        handle.set_max_connections(2000)
        
        # Zero uploads - pure download mode
        handle.set_max_uploads(0)
        
        # Disable sequential download - let seeders send any pieces
        handle.set_sequential_download(False)
        
        # Force announce to find more seeders
        handle.force_reannounce()
        
        # If metadata available, optimize piece priorities
        if handle.has_metadata():
            torrent_info = handle.torrent_file()
            num_pieces = torrent_info.num_pieces()
            
            # Create smart piece priority for zero-leech
            # High priority for first/last pieces, random for middle
            import random
            piece_priorities = []
            
            for i in range(num_pieces):
                if i < 5 or i >= num_pieces - 5:  # First/last 5 pieces
                    piece_priorities.append(7)  # Highest priority
                else:
                    piece_priorities.append(random.randint(3, 6))  # Random medium-high
            
            handle.prioritize_pieces(piece_priorities)
    
    def handle_zero_leech_stall(self, handle):
        """Handle stalled download in zero-leech scenario"""
        
        self.logger.warning("🔄 Zero-leech stall detected - applying recovery")
        
        # Aggressive recovery tactics
        handle.force_reannounce()
        handle.scrape_tracker()
        
        # Reset and randomize piece priorities
        if handle.has_metadata():
            num_pieces = handle.torrent_file().num_pieces()
            import random
            
            # Completely randomize piece priorities to break patterns
            priorities = [random.randint(1, 7) for _ in range(num_pieces)]
            handle.prioritize_pieces(priorities)
        
        # Temporarily increase connection limit
        handle.set_max_connections(3000)
        
        # Force DHT announce if available
        try:
            handle.force_dht_announce()
        except:
            pass
    
    def _apply_ultra_performance_settings(self):
        """Apply ultra-performance settings for maximum speed"""
        try:
            # Additional libtorrent optimizations
            alerts = self.session.pop_alerts()
            
            # Enable all performance-related alerts (handle deprecation)
            try:
                # Try new method first (libtorrent 2.0+)
                settings = self.session.get_settings()
                if hasattr(lt, 'alert_category_t'):
                    alert_mask = (
                        lt.alert_category_t.error_notification |
                        lt.alert_category_t.peer_notification |
                        lt.alert_category_t.performance_warning |
                        lt.alert_category_t.stats_notification
                    )
                else:
                    alert_mask = (
                        lt.alert.category_t.error_notification |
                        lt.alert.category_t.peer_notification |
                        lt.alert.category_t.performance_warning |
                        lt.alert.category_t.stats_notification
                    )
                
                if hasattr(self.session, 'set_alert_mask'):
                    self.session.set_alert_mask(alert_mask)
                else:
                    # For newer versions, alerts are enabled by default
                    pass
            except Exception as e:
                self.logger.warning(f"Could not set alert mask: {e}")
            
            # Apply additional session-level optimizations
            settings = self.session.get_settings()
            
            # Ultra-fast piece selection
            settings['piece_extent_affinity'] = 0  # No affinity for faster selection
            settings['suggest_mode'] = 2  # Most aggressive suggestion mode
            settings['prioritize_partial_pieces'] = False  # Don't prioritize partial pieces
            
            # Ultra-aggressive networking
            settings['enable_incoming_tcp'] = True
            settings['enable_outgoing_tcp'] = True
            settings['enable_incoming_utp'] = True
            settings['enable_outgoing_utp'] = True
            settings['mixed_mode_algorithm'] = 0  # Prefer TCP over uTP
            
            # Maximum request pipelining
            settings['max_out_request_queue'] = 1500
            settings['max_allowed_in_request_queue'] = 10000
            
            # Aggressive timeout settings
            settings['handshake_timeout'] = 5
            settings['request_timeout'] = 10
            settings['peer_timeout'] = 20
            
            # Enhanced DHT
            settings['dht_announce_interval'] = 300  # 5 minutes
            settings['max_dht_items'] = 100000
            
            self.session.apply_settings(settings)
            
            self.logger.info("✅ Ultra-performance settings applied")
            
        except Exception as e:
            self.logger.warning(f"Could not apply all ultra-performance settings: {e}")
    
    def track_speed_history(self, info_hash: str, download_speed: float):
        """Track download speed history for performance analysis"""
        current_time = time.time()
        
        if info_hash not in self.speed_history:
            self.speed_history[info_hash] = {
                'speeds': [],
                'timestamps': [],
                'peak_speed': 0,
                'avg_speed': 0
            }
        
        history = self.speed_history[info_hash]
        history['speeds'].append(download_speed)
        history['timestamps'].append(current_time)
        
        # Keep only last 60 measurements (2 minutes at 2-second intervals)
        if len(history['speeds']) > 60:
            history['speeds'] = history['speeds'][-60:]
            history['timestamps'] = history['timestamps'][-60:]
        
        # Update statistics
        history['peak_speed'] = max(history['speeds'])
        history['avg_speed'] = sum(history['speeds']) / len(history['speeds'])
        
        # Update global performance stats
        self.performance_stats['peak_download_speed'] = max(
            self.performance_stats['peak_download_speed'], 
            download_speed
        )
    
    def get_performance_insights(self) -> Dict[str, Any]:
        """Get performance insights and recommendations"""
        insights = {
            'total_session_time': time.time() - self.session_start_time,
            'active_torrents': len(self.active_torrents),
            'peak_speed_mbps': self.performance_stats['peak_download_speed'] / 1024,
            'recommendations': []
        }
        
        # Analyze performance and provide recommendations
        total_current_speed = sum(
            self.get_torrent_status(hash)['download_rate'] 
            for hash in self.active_torrents 
            if self.get_torrent_status(hash)
        )
        
        if total_current_speed < 100:  # Less than 100 KB/s total
            insights['recommendations'].append("🐌 Low speed detected - Consider enabling zero-leech mode")
        
        if len(self.active_torrents) > 10:
            insights['recommendations'].append("⚠️ Many active torrents - Consider sequential downloads")
        
        # Check system resources
        memory = psutil.virtual_memory()
        if memory.percent > 90:
            insights['recommendations'].append("🧠 High memory usage - Reduce cache size")
        
        cpu_percent = psutil.cpu_percent()
        if cpu_percent > 80:
            insights['recommendations'].append("🔥 High CPU usage - Reduce I/O threads")
        
        return insights
    
    def display_speed_graph(self, info_hash: str, width: int = 60):
        """Display ASCII speed graph for a torrent"""
        if info_hash not in self.speed_history:
            return ""
        
        history = self.speed_history[info_hash]
        speeds = history['speeds']
        
        if len(speeds) < 2:
            return "📊 [Insufficient data for graph]"
        
        # Normalize speeds to graph width
        max_speed = max(speeds) if speeds else 1
        min_speed = min(speeds) if speeds else 0
        
        graph = "📊 ["
        
        for speed in speeds[-width:]:  # Show last 'width' measurements
            if max_speed > 0:
                normalized = int((speed / max_speed) * 10)
                if normalized >= 8:
                    graph += "█"
                elif normalized >= 6:
                    graph += "▆"
                elif normalized >= 4:
                    graph += "▄"
                elif normalized >= 2:
                    graph += "▂"
                elif normalized > 0:
                    graph += "▁"
                else:
                    graph += "·"
            else:
                graph += "·"
        
        graph += f"] Peak: {history['peak_speed']:.1f} KB/s, Avg: {history['avg_speed']:.1f} KB/s"
        return graph
    
    def validate_torrent_file(self, torrent_file_path: str) -> Dict[str, Any]:
        """
        Validate and analyze a torrent file
        
        Args:
            torrent_file_path: Path to the .torrent file
            
        Returns:
            Dictionary with validation results and file info
        """
        result = {
            'valid': False,
            'error': None,
            'file_size': 0,
            'info': {}
        }
        
        try:
            # Check file existence
            if not os.path.isfile(torrent_file_path):
                result['error'] = f"File not found: {torrent_file_path}"
                return result
            
            # Get file size
            result['file_size'] = os.path.getsize(torrent_file_path)
            
            # Basic file validation
            if result['file_size'] < 100:
                result['error'] = f"File too small ({result['file_size']} bytes)"
                return result
            
            # Try to read file content
            try:
                with open(torrent_file_path, 'rb') as f:
                    content = f.read(1024)
                    if not content.startswith(b'd'):
                        result['error'] = "Invalid torrent file format (doesn't start with bencoded dict)"
                        return result
            except Exception as e:
                result['error'] = f"Cannot read file: {e}"
                return result
            
            # Try to parse with libtorrent
            try:
                torrent_info = lt.torrent_info(torrent_file_path)
                result['valid'] = True
                result['info'] = {
                    'name': torrent_info.name(),
                    'total_size': torrent_info.total_size(),
                    'num_files': torrent_info.num_files(),
                    'num_pieces': torrent_info.num_pieces(),
                    'piece_length': torrent_info.piece_length(),
                    'comment': torrent_info.comment() if hasattr(torrent_info, 'comment') else '',
                    'creator': torrent_info.creator() if hasattr(torrent_info, 'creator') else '',
                }
                
                # Get trackers
                trackers = []
                try:
                    for tier in torrent_info.trackers():
                        for tracker in tier:
                            trackers.append(tracker.url)
                    result['info']['trackers'] = trackers
                except:
                    result['info']['trackers'] = []
                
            except Exception as e:
                result['error'] = f"libtorrent parsing failed: {e}"
                
        except Exception as e:
            result['error'] = f"Validation failed: {e}"
        
        return result
    
    def repair_torrent_file(self, torrent_file_path: str) -> str:
        """
        Attempt to repair a corrupted torrent file
        
        Args:
            torrent_file_path: Path to the corrupted .torrent file
            
        Returns:
            Path to repaired file or original if repair not needed
        """
        validation = self.validate_torrent_file(torrent_file_path)
        
        if validation['valid']:
            self.logger.info(f"Torrent file is valid: {torrent_file_path}")
            return torrent_file_path
        
        self.logger.warning(f"Attempting to repair torrent file: {validation['error']}")
        
        # Try basic repairs
        try:
            with open(torrent_file_path, 'rb') as f:
                content = f.read()
            
            # Remove common corruption patterns
            # Remove BOM if present
            if content.startswith(b'\xef\xbb\xbf'):
                content = content[3:]
                self.logger.info("Removed UTF-8 BOM")
            
            # Remove null bytes
            original_length = len(content)
            content = content.replace(b'\x00', b'')
            if len(content) != original_length:
                self.logger.info(f"Removed {original_length - len(content)} null bytes")
            
            # Try to find and fix truncated files
            if not content.endswith(b'e'):
                content += b'e'
                self.logger.info("Added missing end marker")
            
            # Write repaired file
            repaired_path = torrent_file_path + '.repaired'
            with open(repaired_path, 'wb') as f:
                f.write(content)
            
            # Validate repaired file
            repaired_validation = self.validate_torrent_file(repaired_path)
            if repaired_validation['valid']:
                self.logger.info(f"Successfully repaired torrent file: {repaired_path}")
                return repaired_path
            else:
                os.unlink(repaired_path)  # Remove failed repair
                raise ValueError(f"Repair failed: {repaired_validation['error']}")
                
        except Exception as e:
            self.logger.error(f"Failed to repair torrent file: {e}")
            raise ValueError(f"Cannot repair torrent file: {e}")
    
    def add_torrent_to_queue(self, torrent_input: str, download_path: Optional[str] = None) -> bool:
        """
        Add a torrent to the download queue
        
        Args:
            torrent_input: Path to .torrent file or magnet link
            download_path: Custom download path (optional)
            
        Returns:
            True if added successfully, False if queue is full
        """
        if len(self.download_queue) >= self.max_queue_size:
            self.logger.warning(f"Queue is full ({self.max_queue_size}), cannot add more torrents")
            return False
        
        torrent_info = {
            'input': torrent_input,
            'download_path': download_path or str(self.download_path),
            'added_time': time.time(),
            'status': 'queued'
        }
        
        self.download_queue.append(torrent_info)
        self.download_stats['total_queued'] += 1
        
        self.logger.info(f"Added to queue: {torrent_input} (Queue size: {len(self.download_queue)})")
        return True
    
    def add_multiple_torrents_to_queue(self, torrent_list: List[str], download_path: Optional[str] = None) -> int:
        """
        Add multiple torrents to the download queue
        
        Args:
            torrent_list: List of torrent files or magnet links
            download_path: Custom download path (optional)
            
        Returns:
            Number of torrents successfully added to queue
        """
        added_count = 0
        
        for torrent_input in torrent_list:
            if self.add_torrent_to_queue(torrent_input, download_path):
                added_count += 1
            else:
                self.logger.warning(f"Failed to add to queue: {torrent_input}")
        
        self.logger.info(f"Added {added_count}/{len(torrent_list)} torrents to queue")
        return added_count
    
    def process_download_queue(self):
        """Process the download queue and start downloads up to max_concurrent_downloads"""
        while (len(self.active_torrents) < self.max_concurrent_downloads and 
               self.download_queue and self.is_running):
            
            torrent_info = self.download_queue.popleft()
            
            try:
                # Start the download
                info_hash = self.download_torrent(torrent_info['input'], torrent_info['download_path'])
                
                # Update torrent progress tracking
                self.torrent_progress[info_hash] = {
                    'input': torrent_info['input'],
                    'download_path': torrent_info['download_path'],
                    'start_time': time.time(),
                    'status': 'downloading',
                    'progress': 0.0,
                    'download_rate': 0.0,
                    'downloaded_bytes': 0,
                    'total_size': 0
                }
                
                self.logger.info(f"Started download from queue: {torrent_info['input']}")
                
            except Exception as e:
                self.logger.error(f"Failed to start download: {torrent_info['input']}: {e}")
                self.download_stats['total_failed'] += 1
    
    def update_torrent_progress(self, info_hash: str, status: Dict[str, Any]):
        """Update progress tracking for a torrent"""
        if info_hash in self.torrent_progress:
            progress_info = self.torrent_progress[info_hash]
            progress_info.update({
                'progress': status.get('progress', 0.0),
                'download_rate': status.get('download_rate', 0.0),
                'downloaded_bytes': status.get('downloaded', 0),
                'total_size': status.get('total_size', 0),
                'last_update': time.time()
            })
    
    def get_download_summary(self) -> Dict[str, Any]:
        """Get summary of download statistics"""
        active_count = len(self.active_torrents)
        queue_count = len(self.download_queue)
        
        total_download_rate = sum(
            self.get_torrent_status(hash).get('download_rate', 0) 
            for hash in self.active_torrents
        )
        
        total_downloaded = sum(
            progress.get('downloaded_bytes', 0) 
            for progress in self.torrent_progress.values()
        )
        
        session_time = time.time() - self.download_stats['session_start']
        
        return {
            'active_downloads': active_count,
            'queued_downloads': queue_count,
            'total_completed': self.download_stats['total_completed'],
            'total_failed': self.download_stats['total_failed'],
            'total_download_rate_kbs': total_download_rate,
            'total_downloaded_bytes': total_downloaded,
            'total_downloaded_gb': total_downloaded / (1024**3),
            'session_time_minutes': session_time / 60,
            'avg_download_rate_kbs': total_downloaded / 1024 / session_time if session_time > 0 else 0
        }
    
    def print_download_summary(self):
        """Print detailed download summary"""
        summary = self.get_download_summary()
        
        print(f"\n{'='*80}")
        print(f"📊 DOWNLOAD SUMMARY")
        print(f"{'='*80}")
        print(f"Active Downloads: {summary['active_downloads']}")
        print(f"Queued Downloads: {summary['queued_downloads']}")
        print(f"Completed: {summary['total_completed']}")
        print(f"Failed: {summary['total_failed']}")
        print(f"Current Speed: {summary['total_download_rate_kbs']:.1f} KB/s")
        print(f"Total Downloaded: {summary['total_downloaded_gb']:.2f} GB")
        print(f"Session Time: {summary['session_time_minutes']:.1f} minutes")
        print(f"Average Speed: {summary['avg_download_rate_kbs']:.1f} KB/s")
        print(f"{'='*80}")
    
    def save_progress_to_file(self, filename: str = "download_progress.json"):
        """Save download progress to JSON file"""
        try:
            progress_data = {
                'session_start': self.download_stats['session_start'],
                'last_update': time.time(),
                'download_stats': self.download_stats,
                'torrent_progress': self.torrent_progress,
                'summary': self.get_download_summary()
            }
            
            with open(filename, 'w') as f:
                json.dump(progress_data, f, indent=2, default=str)
            
            self.logger.info(f"Progress saved to {filename}")
            
        except Exception as e:
            self.logger.error(f"Failed to save progress: {e}")
    
    def load_progress_from_file(self, filename: str = "download_progress.json"):
        """Load download progress from JSON file"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    progress_data = json.load(f)
                
                self.download_stats.update(progress_data.get('download_stats', {}))
                self.torrent_progress.update(progress_data.get('torrent_progress', {}))
                
                self.logger.info(f"Progress loaded from {filename}")
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to load progress: {e}")
            return False
    
    def load_torrents_from_file(self, file_path: str) -> List[str]:
        """
        Load torrent paths from a text file
        
        Args:
            file_path: Path to file containing torrent paths (one per line)
            
        Returns:
            List of torrent paths/magnet links
        """
        torrents = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Validate torrent path/magnet link
                    if line.startswith('magnet:'):
                        torrents.append(line)
                        self.logger.info(f"Loaded magnet link from line {line_num}")
                    elif os.path.isfile(line) and line.endswith('.torrent'):
                        torrents.append(line)
                        self.logger.info(f"Loaded torrent file: {line}")
                    elif line.startswith('http'):
                        # Support HTTP/HTTPS URLs to torrent files
                        torrents.append(line)
                        self.logger.info(f"Loaded torrent URL: {line}")
                    else:
                        self.logger.warning(f"Invalid torrent path on line {line_num}: {line}")
            
            self.logger.info(f"Loaded {len(torrents)} valid torrents from {file_path}")
            return torrents
            
        except FileNotFoundError:
            self.logger.error(f"Torrent list file not found: {file_path}")
            return []
        except Exception as e:
            self.logger.error(f"Error reading torrent list file {file_path}: {e}")
            return []
    
    def create_torrent_list_file(self, torrents: List[str], output_file: str = "torrent_list.txt"):
        """
        Create a torrent list file from a list of torrents
        
        Args:
            torrents: List of torrent paths/magnet links
            output_file: Output file path
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("# Torrent Download List\n")
                f.write("# One torrent path or magnet link per line\n")
                f.write("# Lines starting with # are comments\n\n")
                
                for torrent in torrents:
                    f.write(f"{torrent}\n")
            
            self.logger.info(f"Created torrent list file: {output_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to create torrent list file: {e}")
    
    async def run_downloads_from_file(self, file_path: str, download_path: Optional[str] = None, 
                                    update_interval: float = 2.0, show_speed_graph: bool = False):
        """
        Run downloads from a torrent list file
        
        Args:
            file_path: Path to file containing torrent paths
            download_path: Download directory
            update_interval: Status update interval
            show_speed_graph: Show speed graphs
        """
        try:
            # Load torrents from file
            torrents = self.load_torrents_from_file(file_path)
            
            if not torrents:
                self.logger.error("No valid torrents found in file")
                return
            
            self.logger.info(f"Starting downloads from file: {file_path}")
            self.logger.info(f"Found {len(torrents)} torrents to download")
            
            # Use the multiple downloads method
            await self.run_multiple_downloads(torrents, download_path, update_interval, show_speed_graph)
            
        except Exception as e:
            self.logger.error(f"Error processing torrent file {file_path}: {e}")
            raise

async def main():
    parser = argparse.ArgumentParser(description='High-Performance Torrent Downloader with Queue System')
    parser.add_argument('torrents', nargs='*', help='Path(s) to .torrent file(s) or magnet link(s)')
    parser.add_argument('-f', '--file', dest='torrent_file', 
                       help='File containing torrent paths (one per line)')
    parser.add_argument('-d', '--download-path', default='/projects/data/downloads/nauman/libgen/scimag', 
                       help='Download directory (default: /projects/data/downloads/nauman/libgen/scimag)')
    parser.add_argument('--max-upload', type=int, default=0,
                       help='Maximum upload rate in KB/s (0 = unlimited)')
    parser.add_argument('--max-download', type=int, default=0,
                       help='Maximum download rate in KB/s (0 = unlimited)')
    parser.add_argument('--sequential', action='store_true',
                       help='Download torrents sequentially instead of concurrently')
    parser.add_argument('--zero-leech', action='store_true',
                       help='Force zero-leech optimization mode (ultra-aggressive)')
    parser.add_argument('--show-speed-graph', action='store_true',
                       help='Show real-time ASCII speed graph')
    parser.add_argument('--update-interval', type=float, default=15.0,
                       help='Status update interval in seconds (default: 6.0)')
    parser.add_argument('--max-concurrent', type=int, default=30,
                       help='Maximum concurrent downloads (default: 30)')
    parser.add_argument('--progress-file', default='download_progress.json',
                       help='Progress file to save/load (default: download_progress.json)')
    parser.add_argument('--create-list', dest='create_list_file',
                       help='Create a torrent list file from command line arguments')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.torrents and not args.torrent_file:
        parser.error("You must provide either torrent files/magnet links or use --file option")
    
    # Create downloader instance
    downloader = HighPerformanceTorrentDownloader(
        download_path=args.download_path,
        max_upload_rate=args.max_upload,
        max_download_rate=args.max_download,
        zero_leech_mode=args.zero_leech
    )
    
    # Set max concurrent downloads
    downloader.max_concurrent_downloads = args.max_concurrent
    
    # Handle create list file option
    if args.create_list_file and args.torrents:
        downloader.create_torrent_list_file(args.torrents, args.create_list_file)
        print(f"✅ Created torrent list file: {args.create_list_file}")
        return
    
    system_stats = downloader.get_system_performance_stats()
    
    mode_indicator = "🚨 ZERO-LEECH ULTRA" if args.zero_leech else "⚡ AUTO-DETECT"
    print(f"🚀 High-Performance Torrent Downloader with Queue System")
    print(f"Mode: {mode_indicator}")
    print(f"CPU Cores: {psutil.cpu_count()} | CPU Usage: {system_stats['cpu_usage']:.1f}%")
    print(f"Total RAM: {psutil.virtual_memory().total / (1024**3):.1f} GB | Available: {system_stats['memory_available_gb']:.1f} GB")
    print(f"Download Path: {args.download_path}")
    print(f"Max Concurrent Downloads: {args.max_concurrent}")
    print(f"Progress File: {args.progress_file}")
    print(f"{'='*80}")
    
    try:
        if args.torrent_file:
            # Load torrents from file
            print(f"📁 Loading torrents from file: {args.torrent_file}")
            await downloader.run_downloads_from_file(
                args.torrent_file, 
                args.download_path, 
                args.update_interval, 
                args.show_speed_graph
            )
        elif len(args.torrents) == 1:
            # Single torrent
            await downloader.run_download(args.torrents[0], args.download_path, args.update_interval, args.show_speed_graph)
        else:
            # Multiple torrents - use queue system
            if args.sequential:
                # Download one by one
                for torrent in args.torrents:
                    print(f"\n📥 Starting download: {torrent}")
                    await downloader.run_download(torrent, args.download_path, args.update_interval, args.show_speed_graph)
            else:
                # Download concurrently using queue system
                print(f"\n📥 Starting {len(args.torrents)} downloads with queue system...")
                await downloader.run_multiple_downloads(args.torrents, args.download_path, args.update_interval, args.show_speed_graph)
                
    except KeyboardInterrupt:
        print("\n⏹️  Download interrupted by user")
        # Save final progress
        downloader.save_progress_to_file(args.progress_file)
        downloader.print_download_summary()
    except Exception as e:
        print(f"❌ Error: {e}")
        downloader.save_progress_to_file(args.progress_file)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
