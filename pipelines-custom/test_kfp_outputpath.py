
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
