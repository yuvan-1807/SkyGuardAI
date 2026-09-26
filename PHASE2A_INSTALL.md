# SkyGuard Phase 2A - Installation

This project is being run with Python 3.14 on Windows.

The old requirements pinned pandas 2.0.3, numpy 1.24.3 and scipy 1.10, which predate Python 3.14 support and can make pip attempt a source build. Use the updated requirements in this folder.

Recommended commands:

```bat
python --version
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

Then run:

```bat
python app_new.py
```

In a second terminal:

```bat
python test_ingest.py
```

or:

```bat
python data_injector.py
```

If an existing environment contains incompatible old NumPy/Pandas/SciPy versions, upgrade them with:

```bat
python -m pip install --upgrade "numpy>=2.4,<3" "pandas>=2.3.3,<3" "scipy>=1.16.1,<2" "scikit-learn>=1.8,<2"
```
