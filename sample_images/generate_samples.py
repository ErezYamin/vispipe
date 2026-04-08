#!/usr/bin/env python3
"""Generate synthetic sample images for pipeline testing."""
import os
import cv2
import numpy as np

OUT_DIR = os.path.join(os.path.dirname(__file__))
os.makedirs(OUT_DIR, exist_ok=True)

rng = np.random.default_rng(42)

for i in range(5):
    # Simulate noisy image: gradient + gaussian noise
    base = np.linspace(30, 200, 640, dtype=np.float32)
    img = np.tile(base, (480, 1))
    noise = rng.normal(0, 25, img.shape).astype(np.float32)
    img = np.clip(img + noise, 0, 255).astype(np.uint8)
    # Convert to 3-channel BGR so it's a realistic "raw" color input
    img_bgr = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
    path = os.path.join(OUT_DIR, f"capture_{i+1:03d}.jpg")
    cv2.imwrite(path, img_bgr, [cv2.IMWRITE_JPEG_QUALITY, 85])
    print(f"Created: {path}")

print("Done — 5 sample images generated.")
