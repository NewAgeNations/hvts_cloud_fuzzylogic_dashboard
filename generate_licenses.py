#!/usr/bin/env python3
"""
License Generation Tool - KEEP THIS SECURE!
This tool is for your use only - DO NOT distribute to users.
Generates license keys for the Bitcoin Trading Signal System.
"""

from license_manager import LicenseGenerator, SECRET_KEY
import argparse
import sys
from datetime import datetime


def main():
    """Generate license keys with various options."""
    generator = LicenseGenerator(SECRET_KEY)
    
    parser = argparse.ArgumentParser(
        description="Bitcoin Trading Signal License Generator - FOR ADMIN USE ONLY",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate a permanent license for any machine (email-based only)
  python generate_licenses.py --email user@example.com

  # Generate a machine-locked license (specific computer)
  python generate_licenses.py --email user@example.com --machine ABC123DEF456

  # Generate a 30-day license for any machine
  python generate_licenses.py --email user@example.com --expiry 30

  # Generate a 1-year machine-locked license
  python generate_licenses.py --email user@example.com --machine ABC123DEF456 --expiry 365

  # Interactive mode (prompts for all inputs)
  python generate_licenses.py --interactive

  # Generate license with specific features enabled
  python generate_licenses.py --email user@example.com --features all,advanced

  # Batch generate multiple licenses from a CSV file
  python generate_licenses.py --batch users.csv

  # Show machine ID for a user (useful for support)
  python generate_licenses.py --show-machine-id ABC123DEF456
"""
    )
    
    # Main options
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Interactive mode - prompts for all inputs'
    )
    
    parser.add_argument(
        '--email', '-e',
        type=str,
        help='User email address'
    )
    
    parser.add_argument(
        '--machine', '-m',
        type=str,
        help='Machine ID (leave empty for any machine)'
    )
    
    parser.add_argument(
        '--expiry', '-x',
        type=int,
        help='Expiry in days (0 or omit for permanent)'
    )
    
    parser.add_argument(
        '--features', '-f',
        type=str,
        help='Comma-separated list of features (default: all)'
    )
    
    # Batch processing
    parser.add_argument(
        '--batch', '-b',
        type=str,
        help='CSV file with user data (email,machine_id,expiry_days,features)'
    )
    
    # Utility options
    parser.add_argument(
        '--show-machine-id',
        type=str,
        help='Show formatted machine ID for a given string'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='.',
        help='Directory to save license files (default: current directory)'
    )
    
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Quiet mode - minimal output'
    )
    
    args = parser.parse_args()
    
    # Handle utility commands first
    if args.show_machine_id:
        print(f"\n🔧 Machine ID: {args.show_machine_id}")
        print("\nUse this Machine ID with the --machine parameter")
        return
    
    # Interactive mode
    if args.interactive:
        interactive_generate(generator, args.output_dir, args.quiet)
        return
    
    # Batch mode
    if args.batch:
        batch_generate(generator, args.batch, args.output_dir, args.quiet)
        return
    
    # Single license generation
    if not args.email:
        print("❌ Error: --email is required for license generation")
        print("   Use --interactive for interactive mode, or --help for options")
        sys.exit(1)
    
    # Parse features
    features = None
    if args.features:
        features = [f.strip() for f in args.features.split(',')]
    
    # Generate license
    token = generator.generate(
        user_email=args.email,
        machine_id=args.machine,
        expiry_days=args.expiry if args.expiry != 0 else None,
        features=features
    )
    
    if not args.quiet:
        print(f"\n✅ License generated successfully!")
        print(f"📧 Email: {args.email}")
        if args.machine:
            print(f"💻 Machine ID: {args.machine}")
        if args.expiry:
            expiry_date = (datetime.utcnow() + timedelta(days=args.expiry)).strftime('%Y-%m-%d')
            print(f"📅 Expires: {expiry_date} ({args.expiry} days)")
        else:
            print(f"📅 Expires: Never (permanent)")
        print(f"✨ Features: {', '.join(features) if features else 'all'}")
        print(f"\n📝 License key saved to: license_{args.email.replace('@', '_at_')}.key")


def interactive_generate(generator, output_dir='.', quiet=False):
    """Interactive license generation."""
    print("\n" + "="*60)
    print("🔐 INTERACTIVE LICENSE GENERATOR")
    print("="*60)
    
    # Get user email
    while True:
        email = input("\n📧 User email: ").strip()
        if '@' in email and '.' in email:
            break
        print("❌ Invalid email format. Please try again.")
    
    # Get machine ID (optional)
    machine = input("\n💻 Machine ID (press Enter for any machine): ").strip()
    if machine == '':
        machine = None
        print("   ✓ License will work on ANY machine")
    
    # Get expiry
    expiry_input = input("\n📅 Expiry in days (0 for permanent, default: 0): ").strip()
    try:
        expiry = int(expiry_input) if expiry_input else 0
        if expiry <= 0:
            expiry = None
            print("   ✓ Permanent license")
        else:
            expiry_date = (datetime.utcnow() + timedelta(days=expiry)).strftime('%Y-%m-%d')
            print(f"   ✓ Expires: {expiry_date} ({expiry} days)")
    except ValueError:
        expiry = None
        print("   ✓ Invalid input, using permanent license")
    
    # Get features
    features_input = input("\n✨ Features (comma-separated, default: all): ").strip()
    features = None
    if features_input and features_input.lower() != 'all':
        features = [f.strip() for f in features_input.split(',')]
        print(f"   ✓ Features: {', '.join(features)}")
    else:
        print("   ✓ Features: all")
    
    print("\n" + "="*60)
    confirm = input("Generate this license? (y/N): ").strip().lower()
    
    if confirm == 'y':
        token = generator.generate(
            user_email=email,
            machine_id=machine,
            expiry_days=expiry,
            features=features
        )
        if not quiet:
            print(f"\n✅ License generated successfully!")
            print(f"📧 Email: {email}")
            if machine:
                print(f"💻 Machine ID: {machine}")
            if expiry:
                print(f"📅 Expires: {expiry_date}")
            else:
                print(f"📅 Expires: Never (permanent)")
            print(f"✨ Features: {', '.join(features) if features else 'all'}")
            print(f"\n📝 License key saved to: license_{email.replace('@', '_at_')}.key")
    else:
        print("\n❌ License generation cancelled.")


def batch_generate(generator, csv_file, output_dir='.', quiet=False):
    """Batch generate licenses from CSV file."""
    import csv
    
    print(f"\n📦 Batch generating licenses from: {csv_file}")
    
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        print(f"📊 Found {len(rows)} license(s) to generate")
        
        successful = 0
        failed = 0
        
        for i, row in enumerate(rows, 1):
            try:
                email = row.get('email', '').strip()
                machine = row.get('machine_id', '').strip() or None
                expiry_str = row.get('expiry_days', '').strip()
                features_str = row.get('features', '').strip()
                
                if not email:
                    print(f"  {i:3d}. ❌ Skipping row {i}: No email")
                    failed += 1
                    continue
                
                # Parse expiry
                expiry = None
                if expiry_str and expiry_str.isdigit():
                    expiry = int(expiry_str)
                    if expiry <= 0:
                        expiry = None
                
                # Parse features
                features = None
                if features_str and features_str.lower() != 'all':
                    features = [f.strip() for f in features_str.split(',')]
                
                # Generate license
                token = generator.generate(
                    user_email=email,
                    machine_id=machine,
                    expiry_days=expiry,
                    features=features
                )
                
                # Save to file
                filename = f"license_{email.replace('@', '_at_')}.key"
                filepath = os.path.join(output_dir, filename)
                with open(filepath, 'w') as f:
                    f.write(token)
                
                successful += 1
                
                if not quiet:
                    expiry_text = f"{expiry} days" if expiry else "permanent"
                    machine_text = f"machine: {machine}" if machine else "any machine"
                    print(f"  {i:3d}. ✅ {email} ({expiry_text}, {machine_text})")
                
            except Exception as e:
                print(f"  {i:3d}. ❌ Error: {e}")
                failed += 1
        
        print(f"\n📊 Batch complete: {successful} successful, {failed} failed")
        
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_file}")
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")


def timedelta(days=0):
    """Helper for expiry date calculation."""
    from datetime import timedelta as td
    return td(days=days)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ License generation cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)