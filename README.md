[![codecov](https://codecov.io/gh/ASU-cubesat/cfdp-rs/branch/main/graph/badge.svg?token=BYFWKOEZFT)](https://codecov.io/gh/ASU-cubesat/cfdp-rs)


# cfdp-rs
This project aims to be a feature-complete, cross-platform, open source Rust implementation of the CCSDS File Delivery Protocol (CFDP).

Currently issues with communcation pipes prevents thorough testing on MacOs and Windows platforms.
Development is needed on these platforms to validate functionality and improve stability.

# Optional Features
The following optional features are currently or planned to be impelemented

- [x] CRC PDU validation
- [ ] Metadata Segmentation
- [ ] Data boundary segmentation
- [x] Delayed NAK mode
- [x] Immediate NAK mode
- [ ] Asynchronous NAK mode

# Inter-Agency Tests
This software suite currently implements the following Common Inter-Agency Tests:

- [x] Series F1
- [x] Series F2
- [x] Series F3
- [ ] Series F4
- [ ] Series F5

# Developers
This package includes a `pre-commit` hook for any interested developer to ensure standard formatting and checking.
The hooks can be installed via the python package `pre-commit` if desired.
