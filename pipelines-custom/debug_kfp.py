# debug_kfp.py

import sys
import os
import inspect
from typing import Generic, TypeVar

print("--- Deep Dive into kfp.dsl.types.type_annotations.OutputPath ---")

# Define T_OutputPath locally, as KFP's OutputPath uses a TypeVar
T_OutputPath = TypeVar('T_OutputPath')

try:
    # We re-import kfp here to ensure we're getting the latest state in this script run
    import kfp
    import kfp.dsl.types.type_annotations as _kfp_type_annotations

    output_path_obj = _kfp_type_annotations.OutputPath

    print(f"\n1. OutputPath object: {output_path_obj}")
    print(f"2. Type of OutputPath object: {type(output_path_obj)}")
    print(f"3. Does OutputPath inherit from typing.Generic? {issubclass(output_path_obj, Generic)}") # This should be True for v2
    print(f"4. Is OutputPath a generic type? {hasattr(output_path_obj, '__origin__')}") # This should be True for v2
    print(f"5. Attributes of OutputPath: {dir(output_path_obj)}")

    # Check for __class_getitem__
    print(f"\n6. Does OutputPath have __class_getitem__? {hasattr(output_path_obj, '__class_getitem__')}") # This is what enables subscripting
    if hasattr(output_path_obj, '__class_getitem__'):
        print(f"   __class_getitem__ is: {output_path_obj.__class_getitem__}")
    else:
        print(f"   This is why it's not subscriptable. It's missing __class_getitem__.")

    # Attempt subscripting again to see exact error from within
    print("\n7. Attempting subscripting (expecting TypeError if issue persists):")
    try:
        test_subscript = output_path_obj[str]
        print(f"   SUCCESS: Subscripting worked. Result: {test_subscript}")
    except TypeError as e:
        print(f"   FAILURE: Still TypeError: {e}")
    except Exception as e:
        print(f"   UNEXPECTED ERROR during subscripting: {e}")

    # Check the source file for OutputPath
    try:
        source_file = inspect.getsourcefile(output_path_obj)
        print(f"\n8. Source file for OutputPath: {source_file}")
        expected_path_prefix = '/home/guo/miniconda3/envs/kfp-ultimate-clean-env/lib/python3.10/site-packages/'
        print(f"   Does this match the expected clean environment path? {source_file.startswith(expected_path_prefix)}")
    except Exception as e:
        print(f"   Could not get source file: {e}")

    # Try printing some lines from the source file around the OutputPath definition (if available)
    if source_file and os.path.exists(source_file):
        print("\n9. Looking at source code around OutputPath definition:")
        try:
            with open(source_file, 'r') as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if "class OutputPath" in line:
                    start_line = max(0, i - 5)
                    end_line = min(len(lines), i + 10)
                    print(f"   Found 'class OutputPath' at line {i+1}. Displaying lines {start_line+1} to {end_line}:")
                    for j in range(start_line, end_line):
                        print(f"   {j+1}: {lines[j].strip()}")
                    break
            else:
                print("   'class OutputPath' definition not found in the source file.")
        except Exception as e:
            print(f"   Error reading source file: {e}")

except Exception as e:
    print(f"Error during deep dive: {e}")

print("\n--- End Deep Dive ---")

# Final check of sys.path
print("\n--- sys.path contents (order matters!) ---")
for i, p in enumerate(sys.path):
    print(f"{i}: {p}")
print("--- End sys.path contents ---")

print(f"\nKFP version according to module: {kfp.__version__}")