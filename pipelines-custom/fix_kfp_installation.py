#!/usr/bin/env python3
"""
This script diagnoses KFP installation issues and attempts to fix them.
It specifically focuses on ensuring proper KFP v2.x behavior.
"""
import sys
import subprocess
import importlib
import pkg_resources
from typing import List, Dict, Any, Optional

def print_section(title: str):
    """Print a section header."""
    print(f"\n{'=' * 80}")
    print(f"== {title}")
    print(f"{'=' * 80}")

def run_command(cmd: List[str]) -> str:
    """Run a shell command and return its output."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(cmd)}:")
        print(f"Exit code: {e.returncode}")
        print(f"stderr: {e.stderr}")
        return ""

def check_installed_packages():
    """Check for possible conflicting or outdated KFP-related packages."""
    print_section("Checking Installed Packages")
    
    kfp_related_packages = [
        'kfp', 'kfp-server-api', 'kfp-pipeline-spec', 
        'kubernetes', 'google-cloud-pipeline-components',
        'google-api-python-client', 'google-auth'
    ]
    
    installed_packages = {pkg.key: pkg.version for pkg in pkg_resources.working_set}
    
    print("KFP-related packages installed:")
    for pkg in kfp_related_packages:
        if pkg in installed_packages:
            print(f"- {pkg}: {installed_packages[pkg]}")
        else:
            print(f"- {pkg}: Not installed")
    
    # Check for multiple versions
    print("\nChecking for multiple installed versions of KFP...")
    pip_list = run_command([sys.executable, "-m", "pip", "list"])
    kfp_versions = [line for line in pip_list.split('\n') if line.startswith('kfp ')]
    if len(kfp_versions) > 1:
        print("WARNING: Multiple KFP versions detected!")
        for v in kfp_versions:
            print(f"  {v}")
    else:
        print("OK: Single KFP version detected.")
    
    return installed_packages

def analyze_kfp_installation():
    """Analyze the current KFP installation."""
    print_section("Analyzing KFP Installation")
    
    try:
        import kfp
        import kfp.dsl as dsl
        
        print(f"KFP version: {kfp.__version__}")
        print(f"KFP installed at: {kfp.__file__}")
        
        # Check OutputPath implementation
        is_subscriptable = False
        try:
            test = dsl.OutputPath[str]
            is_subscriptable = True
            print("OutputPath is subscriptable (proper v2.x behavior)")
        except TypeError:
            print("OutputPath is NOT subscriptable (v1.x behavior)")
        
        # Check for key v2 modules
        v2_modules = ['kfp.v2', 'kfp.v2.dsl']
        for module_name in v2_modules:
            try:
                importlib.import_module(module_name)
                print(f"✓ {module_name} is available")
            except ImportError:
                print(f"✗ {module_name} is NOT available")
        
        return {
            'version': kfp.__version__,
            'path': kfp.__file__,
            'is_subscriptable': is_subscriptable
        }
    except ImportError:
        print("KFP is not installed!")
        return None

def recommend_actions(installed_packages: Dict[str, str], kfp_analysis: Optional[Dict[str, Any]]):
    """Recommend actions based on analysis."""
    print_section("Recommended Actions")
    
    if not kfp_analysis:
        print("1. Install KFP v2.x: pip install kfp==2.13.0")
        return
    
    if not kfp_analysis['is_subscriptable']:
        print("Your KFP installation (version", kfp_analysis['version'], 
              ") is not behaving as expected for a v2.x installation.")
        print("\nRecommended actions:")
        print("1. Completely remove all KFP-related packages:")
        print("   pip uninstall -y kfp kfp-server-api kfp-pipeline-spec google-cloud-pipeline-components")
        print("\n2. Install a clean version of KFP v2:")
        print("   pip install kfp==2.13.0")
        print("\n3. If the issue persists, check for conflicts in your Python environment:")
        print("   - Look for multiple versions of KFP modules")
        print("   - Check for custom modifications to KFP source code")
        print("   - Consider creating a fresh conda environment")
    else:
        print("Your KFP installation appears to be working correctly for v2.x!")
        print("You can use the type-annotated syntax: dsl.OutputPath[str]")

def main():
    """Main function."""
    print_section("KFP Installation Diagnostics")
    
    print("Python version:", sys.version)
    print("Python executable:", sys.executable)
    
    installed_packages = check_installed_packages()
    kfp_analysis = analyze_kfp_installation()
    recommend_actions(installed_packages, kfp_analysis)
    
    print_section("Testing KFP OutputPath")
    
    # Test code for OutputPath
    test_code = """
import kfp
from kfp import dsl

print(f"KFP version: {kfp.__version__}")

# This should work for KFP v2.x, but fail for v1.x
try:
    # Try to use OutputPath with type annotation
    @dsl.component
    def test_component(output_path: dsl.OutputPath[str]):
        pass
    
    print("✓ OutputPath type annotation works correctly (v2.x behavior)")
except TypeError as e:
    print(f"✗ OutputPath type annotation failed: {e}")
    print("This indicates v1.x behavior even though you have v2.x installed.")
"""
    
    # Write test code to a temporary file
    with open("test_kfp_outputpath.py", "w") as f:
        f.write(test_code)
    
    # Execute test code
    print("Running test script:")
    subprocess.run([sys.executable, "test_kfp_outputpath.py"])
    
    print("\nIf you need to update your KFP code for v1.x compatibility:")
    print("Change: def prepare_data(data_path: dsl.OutputPath[str]):")
    print("To:     def prepare_data(data_path: dsl.OutputPath):")

if __name__ == "__main__":
    main()