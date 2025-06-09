#!/usr/bin/env python3
# test_imports.py

import sys
import os

# Geçerli dizini Python yoluna ekle
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

def test_imports():
    """Tüm modüllerin import edilebilirliğini test et"""
    modules = [
        # Utils modülleri
        ("utils.config", ["MLFLOW_CONFIG", "MODEL_CONFIG"]),
        ("utils.connections", ["setup_mlflow", "get_spark_session"]),
        ("utils.logger", ["get_logger"]),
        ("utils.spark_utils", ["create_spark_session"]),
        ("utils.mlflow_utils", ["log_to_mlflow"]),
        
        # Data modülleri
        ("data.loader", ["load_smart_meter_data"]),
        ("data.generator", ["generate_full_year_data"]),
        
        # Processing modülleri
        ("processing.aggregation", ["create_system_level_aggregation"]),
        ("processing.features", ["create_advanced_features"]),
        
        # ML modülleri
        ("ml.pipeline", ["create_ml_pipeline"]),
        ("ml.evaluation", ["get_feature_importance"]),
        
        # Ana dosya
        ("system_energy_forecasting", ["main"])
    ]
    
    all_ok = True
    for module_name, attrs in modules:
        try:
            # Modülü import et
            print(f"Importing {module_name}...", end="")
            module = __import__(module_name, fromlist=attrs)
            
            # Özellikleri kontrol et
            for attr in attrs:
                getattr(module, attr)
            
            print(" ✅ OK")
        except Exception as e:
            print(f" ❌ ERROR: {e}")
            all_ok = False
    
    if all_ok:
        print("\n🚀 Tüm modüller başarıyla import edildi!")
    else:
        print("\n⚠️ Bazı modüllerde import hatası var!")

if __name__ == "__main__":
    test_imports()