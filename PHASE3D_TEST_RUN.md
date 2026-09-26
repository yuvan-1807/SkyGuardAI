# Phase 3D test run

Automated checks cover:

- normal reading with two trusted nearby stations;
- isolated temperature deviation at the target station;
- insufficient/expired time-matched neighbour data;
- pipeline integration under `layers.spatial_evidence`;
- advisory-only behavior (legacy final verdict is not promoted by spatial evidence).
