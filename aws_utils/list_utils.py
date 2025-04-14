#!/usr/bin/env python3

import argparse
import pkg_resources
import sys
from colorama import Fore, Style

def get_console_scripts():
    """Get all console scripts from the entry points"""
    scripts = []
    for entry_point in pkg_resources.iter_entry_points(group='console_scripts'):
        if entry_point.dist.key == 'aws-utils':
            scripts.append(entry_point.name)
    return sorted(scripts)

def main():
    parser = argparse.ArgumentParser(description='List all available AWS utility commands')
    # argparse already provides -h/--help by default, so we don't need to add it
    
    # Parse args - this will automatically handle the -h flag
    parser.parse_args()
    
    scripts = get_console_scripts()
    
    print(f"{Fore.CYAN}Available AWS Utility Commands:{Style.RESET_ALL}")
    for script in scripts:
        print(f"  {Fore.GREEN}• {script}{Style.RESET_ALL}")
    
    print(f"\n{Fore.YELLOW}Use any command with -h for more information{Style.RESET_ALL}")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 