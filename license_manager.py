#!/usr/bin/env python3
"""
License Manager for Bitcoin Trading Signal System
Node-locked licensing with optional expiry date
"""

import hashlib
import hmac
import base64
import json
import socket
import uuid
import platform
import subprocess
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List
# Auto-create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Constants - CHANGE THIS TO YOUR OWN UNIQUE SECRET KEY!
# IMPORTANT: Generate a random 32+ byte key and replace this default
SECRET_KEY = b"WnkI3Ujm9YqZyVhG1Mart-hMYcsMEWc0ixhaDgtSsWF7InZlcnNpb24iOiIxLjAiLCJlbWFpbCI6ImNpdHljbHlrQGdtYWlsLmNvbSIsImNyZWF0ZWQiOiIyMDI2LTAzLTExVDEyOjUwOjI0LjEzNDE0NyIsImZlYXR1cmVzIjpbImFsbCJdLCJtYWNoaW5lX2lkIjoiYTFmZGUyNWYzZTU3NGVjMCJ9"
LICENSE_FILE = "license.key"
LICENSE_VERSION = "1.0"


class LicenseManager:
    """Manages license generation and validation"""
    
    def __init__(self, secret_key: bytes = SECRET_KEY):
        self.secret_key = secret_key
        self.license_file = Path(LICENSE_FILE)
    
    def get_machine_id(self) -> str:
        """
        Generate a unique machine identifier based on hardware and OS.
        Combines multiple factors for better uniqueness.
        """
        components = []
        
        # MAC address (primary identifier)
        try:
            mac = uuid.getnode()
            if mac != uuid.getnode():  # Some platforms return 0
                components.append(f"mac:{mac:012x}")
        except:
            pass
        
        # Hostname
        try:
            hostname = socket.gethostname()
            components.append(f"host:{hostname}")
        except:
            pass
        
        # OS-specific identifiers
        system = platform.system()
        
        if system == "Windows":
            # Windows: try to get volume serial number
            try:
                import win32api
                volume = win32api.GetVolumeInformation("C:\\")[1]
                components.append(f"vol:{volume}")
            except:
                pass
            
            # Windows: motherboard ID (requires admin sometimes)
            try:
                result = subprocess.run(
                    'wmic baseboard get serialnumber',
                    capture_output=True, text=True, shell=True
                )
                if result.returncode == 0:
                    serial = result.stdout.strip().split('\n')[-1].strip()
                    if serial and serial != "SerialNumber":
                        components.append(f"mb:{serial}")
            except:
                pass
                
        elif system == "Linux":
            # Linux: machine-id
            try:
                with open('/etc/machine-id', 'r') as f:
                    machine_id = f.read().strip()
                    components.append(f"mid:{machine_id}")
            except:
                pass
                
        elif system == "Darwin":  # macOS
            # macOS: hardware UUID
            try:
                result = subprocess.run(
                    ['ioreg', '-rd1', '-c', 'IOPlatformExpertDevice'],
                    capture_output=True, text=True
                )
                for line in result.stdout.split('\n'):
                    if 'IOPlatformUUID' in line:
                        uuid_str = line.split('"')[-2]
                        components.append(f"uuid:{uuid_str}")
                        break
            except:
                pass
        
        # CPU info (fallback)
        try:
            cpu_info = str(os.cpu_count()) + platform.processor()
            components.append(f"cpu:{hashlib.md5(cpu_info.encode()).hexdigest()[:8]}")
        except:
            pass
        
        # If we have no components, create a fallback
        if not components:
            fallback = f"{platform.node()}-{platform.system()}"
            components.append(f"fallback:{hashlib.md5(fallback.encode()).hexdigest()[:12]}")
        
        # Combine all components into a single hash
        combined = "|".join(components).encode('utf-8')
        machine_hash = hashlib.sha256(combined).hexdigest()
        
        # Return first 16 chars as machine ID
        return machine_hash[:16]
    
    def get_machine_info(self) -> Dict:
        """Get human-readable machine info for display"""
        return {
            'hostname': socket.gethostname(),
            'system': platform.system(),
            'release': platform.release(),
            'machine': platform.machine(),
            'processor': platform.processor(),
            'machine_id': self.get_machine_id()
        }
    
    def generate_license(self, 
                        user_email: str, 
                        machine_id: Optional[str] = None,
                        expiry_days: Optional[int] = None,
                        features: List[str] = None) -> str:
        """
        Generate a license token.
        
        Args:
            user_email: Email of the licensed user
            machine_id: Optional machine ID (if None, generates for any machine)
            expiry_days: Optional number of days until license expires
            features: Optional list of enabled features
            
        Returns:
            License token as string
        """
        payload = {
            'version': LICENSE_VERSION,
            'email': user_email,
            'created': datetime.utcnow().isoformat(),
            'features': features or ['all']
        }
        
        # Add machine binding if specified
        if machine_id:
            payload['machine_id'] = machine_id
        
        # Add expiry if specified
        if expiry_days:
            expiry = datetime.utcnow() + timedelta(days=expiry_days)
            payload['expiry'] = expiry.isoformat()
        
        # Convert payload to JSON
        message = json.dumps(payload, separators=(',', ':')).encode('utf-8')
        
        # Create HMAC signature
        signature = hmac.new(self.secret_key, message, hashlib.sha256).digest()
        
        # Combine signature + message and encode as base64
        token = base64.urlsafe_b64encode(signature + message).decode('ascii')
        
        return token
    
    def validate_license(self, token: Optional[str] = None) -> Tuple[bool, Dict]:
        """
        Validate a license token.
        
        Args:
            token: License token string (if None, reads from license file)
            
        Returns:
            (is_valid, payload_dict)
        """
        # If no token provided, try to read from file
        if token is None:
            if not self.license_file.exists():
                return False, {'error': 'License file not found'}
            try:
                with open(self.license_file, 'r') as f:
                    token = f.read().strip()
            except Exception as e:
                return False, {'error': f'Cannot read license file: {e}'}
        
        try:
            # Decode token
            data = base64.urlsafe_b64decode(token.encode('ascii'))
            signature = data[:32]  # 32 bytes = SHA256
            message = data[32:]
            
            # Verify signature
            expected = hmac.new(self.secret_key, message, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                return False, {'error': 'Invalid signature'}
            
            # Parse payload
            payload = json.loads(message.decode('utf-8'))
            
            # Check version
            if payload.get('version') != LICENSE_VERSION:
                return False, {'error': f'Unsupported license version: {payload.get("version")}'}
            
            # Check expiry
            if 'expiry' in payload:
                expiry = datetime.fromisoformat(payload['expiry'])
                if datetime.utcnow() > expiry:
                    return False, {
                        'error': 'License expired',
                        'expiry': payload['expiry']
                    }
            
            # Check machine binding
            if 'machine_id' in payload:
                current_id = self.get_machine_id()
                if payload['machine_id'] != current_id:
                    return False, {
                        'error': 'License not valid for this machine',
                        'expected': payload['machine_id'],
                        'current': current_id
                    }
            
            # All checks passed
            return True, payload
            
        except Exception as e:
            return False, {'error': f'Validation error: {str(e)}'}
    
    def save_license(self, token: str) -> bool:
        """Save license token to file."""
        try:
            with open(self.license_file, 'w') as f:
                f.write(token)
            return True
        except Exception as e:
            print(f"Error saving license: {e}")
            return False
    
    def remove_license(self) -> bool:
        """Remove license file."""
        try:
            if self.license_file.exists():
                self.license_file.unlink()
            return True
        except Exception as e:
            print(f"Error removing license: {e}")
            return False


class LicenseCLI:
    """Command-line interface for license management"""
    
    def __init__(self):
        self.manager = LicenseManager()
    
    def show_machine_info(self):
        """Display machine information for license generation."""
        info = self.manager.get_machine_info()
        print("\n" + "="*60)
        print("MACHINE INFORMATION".center(60))
        print("="*60)
        print(f"Hostname: {info['hostname']}")
        print(f"System: {info['system']} {info['release']}")
        print(f"Architecture: {info['machine']}")
        print(f"Processor: {info['processor']}")
        print("-"*60)
        print(f"MACHINE ID: {info['machine_id']}")
        print("="*60)
        print("\nSend this Machine ID to receive your license.\n")
    
    def activate_license(self, token: str = None):
        """Activate a license."""
        if token is None:
            token = input("Enter license key: ").strip()
        
        valid, result = self.manager.validate_license(token)
        if valid:
            self.manager.save_license(token)
            print("\n✅ LICENSE ACTIVATED SUCCESSFULLY!")
            print(f"   Licensed to: {result.get('email', 'Unknown')}")
            if 'expiry' in result:
                expiry = datetime.fromisoformat(result['expiry']).strftime('%Y-%m-%d')
                print(f"   Expires: {expiry}")
            print(f"   Features: {', '.join(result.get('features', ['all']))}")
        else:
            print(f"\n❌ License activation failed: {result.get('error', 'Unknown error')}")
    
    def show_status(self):
        """Show current license status."""
        valid, result = self.manager.validate_license()
        
        print("\n" + "="*60)
        print("LICENSE STATUS".center(60))
        print("="*60)
        
        if valid:
            print("✅ STATUS: ACTIVE")
            print(f"   Licensed to: {result.get('email', 'Unknown')}")
            if 'expiry' in result:
                expiry = datetime.fromisoformat(result['expiry']).strftime('%Y-%m-%d')
                days_left = (datetime.fromisoformat(result['expiry']) - datetime.utcnow()).days
                print(f"   Expires: {expiry} ({days_left} days left)")
            else:
                print("   Expires: Never (permanent license)")
            print(f"   Features: {', '.join(result.get('features', ['all']))}")
        else:
            print("❌ STATUS: INACTIVE")
            print(f"   Reason: {result.get('error', 'No valid license')}")
            print("\n   Get a license or activate with: python license_manager.py --activate")
        
        print("="*60)
    
    def deactivate(self):
        """Remove current license."""
        confirm = input("Are you sure you want to deactivate this license? (y/N): ")
        if confirm.lower() == 'y':
            if self.manager.remove_license():
                print("✅ License deactivated.")
            else:
                print("❌ Failed to deactivate license.")


# ============================================================================
# License generation tool (for your use only - NOT distributed to users)
# ============================================================================

class LicenseGenerator:
    """Tool for generating licenses (should be kept separate from distributed code)"""
    
    def __init__(self, secret_key: bytes):
        self.manager = LicenseManager(secret_key)
    
    def generate(self, 
                user_email: str,
                machine_id: Optional[str] = None,
                expiry_days: Optional[int] = None,
                features: List[str] = None):
        """Generate and display a license."""
        token = self.manager.generate_license(
            user_email=user_email,
            machine_id=machine_id,
            expiry_days=expiry_days,
            features=features
        )
        
        print("\n" + "="*80)
        print("GENERATED LICENSE".center(80))
        print("="*80)
        print(f"User: {user_email}")
        if machine_id:
            print(f"Machine ID: {machine_id}")
        if expiry_days:
            expiry = (datetime.utcnow() + timedelta(days=expiry_days)).strftime('%Y-%m-%d')
            print(f"Expires: {expiry}")
        print(f"Features: {features or ['all']}")
        print("-"*80)
        print(token)
        print("="*80)
        
        # Optionally save to file
        filename = f"license_{user_email.replace('@', '_at_')}.key"
        with open(filename, 'w') as f:
            f.write(token)
        print(f"\n✅ License saved to: {filename}")
        
        return token


# ============================================================================
# Main entry point for license management CLI
# ============================================================================

def main():
    """Command-line interface for license management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bitcoin Trading Signal License Manager')
    parser.add_argument('--info', action='store_true', help='Show machine information')
    parser.add_argument('--status', action='store_true', help='Show license status')
    parser.add_argument('--activate', metavar='KEY', nargs='?', const='prompt', 
                       help='Activate license (provide key or enter interactively)')
    parser.add_argument('--deactivate', action='store_true', help='Remove current license')
    
    # Hidden generation options (for your use only)
    parser.add_argument('--generate', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--email', help=argparse.SUPPRESS)
    parser.add_argument('--machine', help=argparse.SUPPRESS)
    parser.add_argument('--expiry', type=int, help=argparse.SUPPRESS)
    
    args = parser.parse_args()
    
    cli = LicenseCLI()
    
    # Hidden generation mode
    if args.generate:
        if not args.email:
            print("Error: --email required for license generation")
            return
        generator = LicenseGenerator(SECRET_KEY)
        generator.generate(
            user_email=args.email,
            machine_id=args.machine,
            expiry_days=args.expiry
        )
        return
    
    # Normal user commands
    if args.info:
        cli.show_machine_info()
    elif args.status:
        cli.show_status()
    elif args.activate:
        if args.activate == 'prompt':
            cli.activate_license()
        else:
            cli.activate_license(args.activate)
    elif args.deactivate:
        cli.deactivate()
    else:
        # Default: show status
        cli.show_status()


if __name__ == "__main__":
    main()