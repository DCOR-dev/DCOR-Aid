from dcoraid.api import s3_api


import pytest

GiB = 1024**3
MiB = 1024**2


@pytest.mark.parametrize("file_size,expected", [
    [100,
     {"num_parts": 1,
      "part_size": 100,
      "part_size_last": 100,
      "file_size": 100,
      }
     ],
    [2 * GiB,
     {"num_parts": 2,
      "part_size": GiB,
      "part_size_last": GiB,
      "file_size": 2*GiB,
      }
     ],
    [2 * GiB - 2,
     {"num_parts": 2,
      "part_size": GiB - 1,
      "part_size_last": GiB - 1,
      "file_size": 2 * GiB - 2,
      }
     ],
    [GiB + 2,
     {"num_parts": 2,
      "part_size": GiB // 2 + 1,
      "part_size_last": GiB // 2 + 1,
      "file_size": GiB + 2,
      }
     ],
])
def test_compute_upload_part_parameters(file_size, expected):
    actual = s3_api.compute_upload_part_parameters(file_size)
    assert expected == actual
