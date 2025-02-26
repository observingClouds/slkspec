import os
from pathlib import Path

import pyslk

test_files = [
    dict(
        name=(
            "/arch/bk1040/dyamond_winter_post_processed/ECMWF/IFS-4km/DW-CPL/"
            + "atmos/1hr/tas/r1i1p1f1/2d/gn/tas_1hr_IFS-4km_DW-CPL_r1i1p1f1_2"
            + "d_gn_20200220000000-20200220230000.nc"
        ),
        size=0.8215,
        query=(
            '{"$and":[{"path":{"$gte":"/arch/bk1040/dyamond_winter_post_proce'
            + 'ssed/ECMWF/IFS-4km/DW-CPL/atmos/1hr/tas/r1i1p1f1/2d/gn","$max_'
            + 'depth":1}},{"resources.name":{"$regex":"tas_1hr_IFS-4km_DW-CPL'
            + '_r1i1p1f1_2d_gn_20200220000000-20200220230000.nc"}}]}'
        ),
    ),
    dict(
        name=(
            "/arch/bk1040/dyamond_winter_post_processed/ECMWF/IFS-4km/DW-CPL/"
            + "atmos/1hr/tas/r1i1p1f1/2d/gn/tas_1hr_IFS-4km_DW-CPL_r1i1p1f1_2"
            + "d_gn_2020022900000d_gn_20200229000000-20200229000000.nc"
        )
    ),
]


def test_retrieve_improved() -> None:
    import tempfile

    filename = "/arch/bm0146/k204221/iow/INDEX.txt"
    td = tempfile.TemporaryDirectory()
    pyslk.retrieve_improved(filename, td.name, preserve_path=True)
    assert os.stat(Path(td.name, filename[1:])).st_size == 1268945
    td.cleanup()


def test_search() -> None:
    assert isinstance(pyslk.search(test_files[0]["query"]), int)
