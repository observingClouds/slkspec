import pyslk
import os
from pathlib import Path

test_files = [
            dict(
                name="/arch/bk1040/dyamond_winter_post_processed/ECMWF/IFS-4km/DW-CPL/atmos/1hr/tas/r1i1p1f1/2d/gn/tas_1hr_IFS-4km_DW-CPL_r1i1p1f1_2d_gn_20200220000000-20200220230000.nc",
                size=0.8215,
                query='{"$and":[{"path":{"$gte":"/arch/bk1040/dyamond_winter_post_processed/ECMWF/IFS-4km/DW-CPL/atmos/1hr/tas/r1i1p1f1/2d/gn","$max_depth":1}},{"resources.name":{"$regex":"tas_1hr_IFS-4km_DW-CPL_r1i1p1f1_2d_gn_20200220000000-20200220230000.nc"}}]}',
            ),
            dict(
                name="/arch/bk1040/dyamond_winter_post_processed/ECMWF/IFS-4km/DW-CPL/atmos/1hr/tas/r1i1p1f1/2d/gn/tas_1hr_IFS-4km_DW-CPL_r1i1p1f1_2d_gn_20200229000000-20200229000000.nc"
            ),
        ]

def test_gen_file_query() -> None:
    query = pyslk.gen_file_query(test_files[0]["name"])
    assert (query == test_files[0]["query"])

def test_retrieve() -> None:
    import tempfile

    filename = "/arch/bm0146/k204221/iow/INDEX.txt"
    td = tempfile.TemporaryDirectory()
    pyslk.retrieve(filename, td.name, preserve_path=True)
    assert(os.stat(Path(td.name, filename[1:])).st_size == 1268945)
    td.cleanup()

def test_search() -> None:
    assert (isinstance(pyslk.search(test_files[0]['query']), int))


def test_slk_list() -> None:
    assert (pyslk.slk_list('/arch/bm0146/k204221/iow/') == 
            '-rwxr-xr-x- k204221     bm0146          1.2M   10 Jun 2020 08:25 INDEX.txt\n-rw-r--r--t k204221     bm0146         19.5G   05 Jun 2020 17:36 iow_data2_001.tar\n-rw-r--r--t k204221     bm0146         19.0G   05 Jun 2020 17:38 iow_data2_002.tar\n-rw-r--r--t k204221     bm0146         19.4G   05 Jun 2020 17:38 iow_data2_003.tar\n-rw-r--r--t k204221     bm0146         19.3G   05 Jun 2020 17:40 iow_data2_004.tar\n-rw-r--r--t k204221     bm0146         19.1G   05 Jun 2020 17:40 iow_data2_005.tar\n-rw-r--r--t k204221     bm0146          7.8G   05 Jun 2020 17:41 iow_data2_006.tar\n-rw-r--r--t k204221     bm0146        186.9G   05 Jun 2020 19:37 iow_data3_001.tar\n-rw-r--r--t k204221     bm0146         24.6G   05 Jun 2020 19:14 iow_data3_002.tar\n-rw-r--r--- k204221     bm0146          4.0M   05 Jun 2020 19:43 iow_data4_001.tar\n-rw-r--r--t k204221     bm0146         10.5G   05 Jun 2020 19:46 iow_data4_002.tar\n-rw-r--r--t k204221     bm0146         19.5G   10 Jun 2020 08:21 iow_data5_001.tar\n-rw-r--r--t k204221     bm0146         19.0G   10 Jun 2020 08:23 iow_data5_002.tar\n-rw-r--r--t k204221     bm0146         19.4G   10 Jun 2020 08:23 iow_data5_003.tar\n-rw-r--r--t k204221     bm0146         19.3G   10 Jun 2020 08:24 iow_data5_004.tar\n-rw-r--r--t k204221     bm0146         19.1G   10 Jun 2020 08:25 iow_data5_005.tar\n-rw-r--r--t k204221     bm0146          7.8G   10 Jun 2020 08:25 iow_data5_006.tar\n-rw-r--r--t k204221     bm0146         19.5G   05 Jun 2020 17:53 iow_data_001.tar\n-rw-r--r--t k204221     bm0146         19.0G   05 Jun 2020 17:53 iow_data_002.tar\n-rw-r--r--t k204221     bm0146         19.4G   05 Jun 2020 17:56 iow_data_003.tar\n-rw-r--r--t k204221     bm0146         19.3G   05 Jun 2020 17:56 iow_data_004.tar\n-rw-r--r--t k204221     bm0146         19.1G   05 Jun 2020 17:58 iow_data_005.tar\n-rw-r-----t k204221     bm0146          7.8G   05 Jun 2020 17:57 iow_data_006.tar\nFiles: 23\n\x1b[?25h')
    pass

