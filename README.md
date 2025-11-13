
MobilityNebula
===============

An open-source geospatial trajectory data streaming platform based on [NebulaStream](https://nebula.stream/).

<img src="docs/images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

MobilityNebula explores the advantages of [MobilityDB](https://github.com/MobilityDB/MobilityDB) datatypes and functions in the NebulaStream environment, using the [MEOS](https://libmeos.org/) library as middleware.

The MobilityDB project is developed by the Computer & Decision Engineering Department of the [Université libre de Bruxelles](https://www.ulb.be/) (ULB) under the direction of [Prof. Esteban Zimányi](http://cs.ulb.ac.be/members/esteban/). ULB is an OGC Associate Member and member of the OGC Moving Feature Standard Working Group ([MF-SWG](https://www.ogc.org/projects/groups/movfeatswg)).

<img src="docs/images/OGC_Associate_Member_3DR.png" width="100" alt="OGC Associate Member Logo" />

More information about MobilityDB, including publications, presentations, etc., can be found in the MobilityDB [website](https://mobilitydb.com).

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="docs/resources/NebulaBanner.png">
    <source media="(prefers-color-scheme: dark)" srcset="docs/resources/NebulaBannerDarkMode.png">
    <img alt="NebulaStream logo" src="docs/resources/NebulaBanner.png" height="100">
  </picture>
  <br />
  <!-- Badges -->
  <a href="https://github.com/nebulastream/nebulastream-public/actions/workflows/nightly.yml">
    <img src="https://github.com/nebulastream/nebulastream-public/actions/workflows/nightly.yml/badge.svg"
         alt="NES Nightly" />
  </a>
  <a href="https://bench.nebula.stream/c-benchmarks/">
    <img src="https://img.shields.io/badge/Benchmark-Conbench-blue?labelColor=3D444C"
         alt="Conbench" />
  </a>
</div>

----
NebulaStream is TU Berlin/ BIFOLD attempt to develop a general-purpose, end-to-end data management system for the IoT.
It provides an out-of-the-box experience with rich data processing functionalities and a high ease-of-use.

NebulaStream is a joint research project between the DIMA group at TU Berlin and BIFOLD.

Learn more about Nebula Stream at https://www.nebula.stream or take a look at our [documentation](docs).

# Clang-Format, Clang-Tidy, License & Pragma Once Check, and Ensure Correct Comments
We use `clang-format` and `clang-tidy` to ensure code quality and consistency.
To run the checks, you can use the target `format`. 

## Project Structure
Check the [Project Structure](docs/project_structure.md) to understand the dependency between components. 

## Development
NebulaStream targets C++23 using all features implemented in both `libc++` 18 and `libstdc++` 13.2. All tests are using
`Clang` 18.
Follow the [development guide](docs/technical/development.md) to learn how to set up the development environment.
To see our code of conduct, please refer to [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md).

## Build Types
This project supports multiple build types to cater to different stages of development and deployment. Below are the details of each build type:

### Debug
- **Default Logging Level**: All logging messages are compiled.
- **Assert Checks**: Enabled.
- **Use Case**: Ideal for development, providing comprehensive logging and assert checks to help identify and fix issues.

### RelWithDebInfo (Release with Debug Information)
- **Default Logging Level**: Warning.
- **Assert Checks**: Enabled.
- **Use Case**: Balances performance and debugging, including warning-level logging and assert checks for useful debugging information without full logging overhead.

### Release
- **Default Logging Level**: Error.
- **Assert Checks**: Enabled.
- **Use Case**: Optimized for performance, with logging set to error level and assert checks disabled, ensuring only critical issues are logged.

### Benchmark
- **Logging Level**: None.
- **Assert Checks**: Disabled.
- **Use Case**: Designed for maximum performance, omitting all logging and assert checks, including null pointer checks. Suitable for thoroughly tested environments where performance is critical.
- Use this with care, as this is not regularly tested, i.e., Release terminates deterministically if a bug occurs (failed invariant/precondition), whereas Benchmark will be in an undefined state.



# Publications
[1] Mariana M. Garcez Duarte, Dwi P. A. Nugroho, Georges Tod, Evert Bevernage, Pieter Moelans, Emine Tas, Esteban Zimányi, Mahmoud Sakr, Steffen Zeuch, and Volker Markl. 2025. Mobility Stream Processing on NebulaStream and MEOS. In Companion of the 2025 International Conference on Management of Data (SIGMOD/PODS '25). Association for Computing Machinery, New York, NY, USA, 79–82. https://doi.org/10.1145/3722212.3725116

[2] Mariana M. Garcez Duarte, Dwi P. A. Nugroho, Georges Tod, Evert Bevernage, Pieter Moelans, Elias Saerens, Esteban Zimányi, Mahmoud Sakr, Steffen Zeuch. 2025. 
Mobility Data Stream Processing Beyond the Cloud. 33rd ACM SIGSPATIAL
International Conference on Advances in Geographic Information Systems
(ACM SIGSPATIAL 2025).


# Acknowledgements
This work was supported in part by the Horizon Framework Programme of the European Union under grant agreement No. 101070279 ([MobiSpaces](https://mobispaces.eu)).