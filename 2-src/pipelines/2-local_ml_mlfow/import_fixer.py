#!/usr/bin/env python3
# import_fixer.py

import os
import re
from pathlib import Path

def fix_imports(directory):
    """Tüm Python dosyalarındaki göreceli importları düzelt"""
    print(f"Dosyalar taranıyor: {directory}")
    count = 0
    
    for file_path in Path(directory).glob('**/*.py'):
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Göreceli importları tespit et
        if re.search(r'from\s+\.\.', content) or re.search(r'from\s+\.', content) or re.search(r'pipelines\.2-local', content):
            print(f"Düzeltiliyor: {file_path}")
            
            # Geçersiz modül adı düzeltme (pipelines)
            new_content = re.sub(
                r'pipelines\.2-local_ml_mlfow',
                r'pipelines',
                content
            )
            
            # İki noktalı göreceli importları değiştir (from ..utils.xxx)
            new_content = re.sub(
                r'from\s+\.\.([a-zA-Z0-9_.]+)\s+import\s+([a-zA-Z0-9_,\s]+)',
                r'from utils\1 import \2',
                new_content
            )
            
            # Tek noktalı göreceli importları değiştir (from .xxx)
            folder_name = file_path.parent.name
            new_content = re.sub(
                r'from\s+\.([a-zA-Z0-9_]+)\s+import\s+([a-zA-Z0-9_,\s]+)',
                rf'from {folder_name}.\1 import \2',
                new_content
            )
            
            with open(file_path, 'w') as f:
                f.write(new_content)
            
            count += 1
    
    print(f"Toplam {count} dosya düzeltildi.")

if __name__ == "__main__":
    fix_imports("/workspace/pipelines/2-local_ml_mlfow")